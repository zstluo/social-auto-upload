# -*- coding: utf-8 -*-
from datetime import datetime
from playwright.async_api import Playwright, async_playwright, Page
import os
import re
import asyncio
from typing import List, Tuple, Optional

from conf import LOCAL_CHROME_PATH
from utils.base_social_media import set_init_script
from utils.log import douyin_logger


# ---------------------------
# 基础：cookie 检测 / 生成
# ---------------------------
async def cookie_auth(account_file: str, headless: bool = True) -> bool:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=headless)
        context = await browser.new_context(storage_state=account_file)
        context = await set_init_script(context)
        page = await context.new_page()
        try:
            await page.goto("https://creator.douyin.com/creator-micro/content/upload")
            await page.wait_for_url("**/creator-micro/content/upload", timeout=15000)
        except Exception:
            douyin_logger.info("[cookie] cookie 失效")
            await context.close()
            await browser.close()
            return False

        # 登录页判断（2024.06 后新版）
        if await page.get_by_text('手机号登录').count() or await page.get_by_text('扫码登录').count():
            douyin_logger.info("[cookie] cookie 失效")
            await context.close()
            await browser.close()
            return False

        douyin_logger.info("[cookie] cookie 有效")
        await context.close()
        await browser.close()
        return True


async def douyin_setup(account_file: str, handle: bool = False, account_alias: str = "", headless: bool = True) -> bool:
    if os.path.exists(account_file) and await cookie_auth(account_file, headless=headless):
        return True

    if not handle:
        return False

    douyin_logger.info(f"[cookie] 为账号 {account_alias or 'default'} 创建/更新 cookie，请扫码登录")
    await douyin_cookie_gen(account_file, headless=False)
    return True


async def douyin_cookie_gen(account_file: str, headless: bool = False) -> None:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=headless)
        context = await browser.new_context()
        context = await set_init_script(context)

        page = await context.new_page()
        await page.goto("https://creator.douyin.com/")
        await page.pause()  # 你点继续后会保存 cookie
        await context.storage_state(path=account_file)

        await context.close()
        await browser.close()


# ---------------------------
# .txt 读取与覆盖
# 1 行：标题
# 2 行：话题（逗号/空格/中文逗号分隔；可写 #xxx）
# 3 行：商品链接
# 4 行：商品短标题
# ---------------------------
def read_txt_payload(txt_path: Optional[str]) -> Tuple[Optional[str], List[str], Optional[str], Optional[str]]:
    if not txt_path:
        return None, [], None, None
    if not os.path.exists(txt_path):
        douyin_logger.warning(f"[txt] 文件不存在：{txt_path}")
        return None, [], None, None

    with open(txt_path, "r", encoding="utf-8") as f:
        raw_lines = [line.strip() for line in f.readlines() if line.strip() != ""]

    title = None
    tags: List[str] = []
    product_url = None
    product_title = None

    if len(raw_lines) >= 1:
        title = raw_lines[0][:1000]  # 抖音标题上限兜底

    if len(raw_lines) >= 2:
        # 支持 "#旅游 #美食" 或 "旅游,美食" 或 "旅游 美食"
        tline = raw_lines[1]
        tline = tline.replace("，", ",").replace("、", ",").replace("；", ",").replace(";", ",")
        parts = re.split(r"[,\s]+", tline)
        tags = [p.lstrip("#").strip() for p in parts if p.strip()]

    if len(raw_lines) >= 3:
        product_url = raw_lines[2].strip()

    if len(raw_lines) >= 4:
        product_title = raw_lines[3].strip()[:10]  # 商品短标题 ≤10 字

    # 只给了两行 -> 不挂车
    if len(raw_lines) == 2:
        product_url = None
        product_title = None

    return title, tags, product_url, product_title


# ---------------------------
# DouYinVideo 主流程
# ---------------------------
class DouYinVideo(object):
    def __init__(
        self,
        title: str,
        file_path: str,
        tags: List[str],
        publish_date: datetime,
        account_file: str,
        thumbnail_path: str = None,
        product_url: str = None,
        product_title: str = None,
        txt_path: str = None,          # 新增：可传 .txt 一键覆盖
        headless: bool = False,        # 可视化/无头切换（CLI 会传）
    ):
        # 先用 cli 传入的
        self.title = title
        self.file_path = file_path
        self.tags = tags or []
        self.publish_date = publish_date
        self.account_file = account_file
        self.thumbnail_path = thumbnail_path
        self.product_url = product_url
        self.product_title = product_title
        self.date_format = '%Y年%m月%d日 %H:%M'
        self.local_executable_path = LOCAL_CHROME_PATH
        self.headless = headless

        # 若给了 .txt，覆盖字段
        t_title, t_tags, t_url, t_short = read_txt_payload(txt_path)
        if t_title: self.title = t_title
        if t_tags: self.tags = t_tags
        # 判断：只有两行就不挂车；四行就挂车
        if t_url and t_short:
            self.product_url = t_url
            self.product_title = t_short
        elif txt_path:
            # 明确告知：只给了两行/或不完整，视为不挂车
            if t_url or t_short:
                douyin_logger.warning("[txt] 购物车信息不完整（需要 4 行），此次不挂车")
            else:
                douyin_logger.info("[txt] 检测到 2 行文本（标题/话题），此次不挂车")

    # ---------- 组件方法 ----------
    async def set_schedule_time_douyin(self, page: Page, publish_date: datetime):
        label_element = page.locator("[class^='radio']:has-text('定时发布')")
        await label_element.click()
        await asyncio.sleep(0.2)
        publish_date_hour = publish_date.strftime("%Y-%m-%d %H:%M")
        await page.locator('.semi-input[placeholder="日期和时间"]').click()
        await page.keyboard.press("Control+A")
        await page.keyboard.type(str(publish_date_hour))
        await page.keyboard.press("Enter")
        await asyncio.sleep(0.2)

    async def handle_upload_error(self, page: Page):
        douyin_logger.info('视频出错了，重新上传中')
        await page.locator('div.progress-div [class^="upload-btn-input"]').set_input_files(self.file_path)

    async def add_product(self, page: Page):
        """
        添加商品（购物车）：
        - 正常：出现【编辑商品】 → 填“商品短标题” → 完成编辑 → (True, "ok")
        - 若出现【无法添加购物车】→ 终止（不发布），落地截图/HTML → (False, "quota_reached")
        - 异常 → 落地截图/HTML → (False, "error")
        """
        if not self.product_url:
            douyin_logger.info('  [-] 未提供商品链接，跳过加商品')
            return True, "skipped"

        douyin_logger.info('  [-] 正在添加商品...')
        try:
            # 1) 找到“扩展信息”卡片和“添加标签”块
            ext_card = page.locator("css=div:has(> .title-bu2hyo:has-text('扩展信息'))").first
            await ext_card.wait_for(state='attached', timeout=8000)
            await ext_card.scroll_into_view_if_needed()

            tag_block = ext_card.locator(
                "css=div:has(.title-dS7kae .title-content-oaqcSp:has-text('添加标签'))"
            ).first
            await tag_block.wait_for(state='attached', timeout=8000)
            await tag_block.scroll_into_view_if_needed()

            # 2) 左侧下拉 → 购物车
            type_select = tag_block.locator(".semi-select").first
            await type_select.wait_for(state='visible', timeout=8000)
            await type_select.click()

            option_cart = page.locator(
                "[role='listbox'] [role='option']:has-text('购物车'), .semi-select-option:has-text('购物车')"
            ).first
            await option_cart.click()

            async def is_cart_selected() -> bool:
                try:
                    return "购物车" in (await type_select.inner_text())
                except Exception:
                    return False

            if not await is_cart_selected():
                await type_select.click()
                await option_cart.click()
            if not await is_cart_selected():
                raise Exception("切换到“购物车”失败")

            # 3) 填链接 + 点“添加链接”
            input_scope = tag_block.locator("#douyin_creator_pc_anchor_jump").first
            url_input = input_scope.locator("input, textarea, .semi-input input").first
            await url_input.wait_for(state='visible', timeout=10000)
            await url_input.click()
            try: await page.keyboard.press("Control+A")
            except Exception: pass
            await page.keyboard.press("Backspace")
            await url_input.fill(self.product_url)

            add_btn = tag_block.get_by_text("添加链接", exact=False).first
            await add_btn.click()

            # 4) 等待两种结果之一
            edit_dialog = page.locator("div[role='dialog']:has-text('编辑商品')").first
            quota_dialog = page.locator("div[role='dialog']:has-text('无法添加购物车')").first

            # 优先等编辑弹窗
            try:
                await edit_dialog.wait_for(timeout=8000)
                # 正常编辑：填商品短标题 → 完成编辑
                short_title = edit_dialog.locator(
                    "input[placeholder*='商品短标题'], textarea[placeholder*='商品短标题'], "
                    "input[placeholder*='短标题'], textarea[placeholder*='短标题']"
                )
                if await short_title.count():
                    await short_title.first.click()
                    try: await page.keyboard.press("Control+A")
                    except Exception: pass
                    await page.keyboard.press("Backspace")
                    await short_title.first.fill((self.product_title or self.title or "同款")[:10])

                finish_btn = edit_dialog.get_by_role("button", name=re.compile("完成编辑|完成")).or_(
                    edit_dialog.get_by_text("完成编辑", exact=False)
                )
                await finish_btn.first.click()
                douyin_logger.success('  [-] 商品添加完成')
                return True, "ok"

            except Exception:
                # 检查是否配额限制
                if await quota_dialog.count():
                    douyin_logger.error("  [×] 无法添加购物车：今日/当周额度已满 → 本次发布已停止")
                    # 记录页面，便于复盘
                    try:
                        await page.screenshot(path='add_product_error.png', full_page=True)
                        with open('full_page.html', 'w', encoding='utf-8') as f:
                            f.write(await page.content())
                    except Exception:
                        pass
                    # 可选择点击“取消/查看规则/×”，这里不必强行关闭
                    return False, "quota_reached"

                # 其它未知情况
                raise

        except Exception as e:
            douyin_logger.error(f'  [-] 商品添加失败: {e}')
            try:
                await page.screenshot(path='add_product_error.png', full_page=True)
                with open('full_page.html', 'w', encoding='utf-8') as f:
                    f.write(await page.content())
            except Exception:
                pass
            return False, "error"

    # ---------- 主上传 ----------
    async def upload(self, playwright: Playwright) -> None:
        # 启动浏览器
        if self.local_executable_path:
            browser = await playwright.chromium.launch(
                headless=self.headless, executable_path=self.local_executable_path
            )
        else:
            browser = await playwright.chromium.launch(headless=self.headless)

        # 用 cookie 创建上下文；提前给 geolocation 权限，避免弹窗挡表单
        context = await browser.new_context(storage_state=f"{self.account_file}")
        context = await set_init_script(context)
        await context.grant_permissions(["geolocation"], origin="https://creator.douyin.com")
        await context.set_geolocation({"latitude": 30.2741, "longitude": 120.1551})

        page = await context.new_page()
        await page.goto("https://creator.douyin.com/creator-micro/content/upload")
        douyin_logger.info(f'[+]正在上传: {os.path.basename(self.file_path)}')
        douyin_logger.info('[-] 正在打开主页...')
        await page.wait_for_url("**/creator-micro/content/upload", timeout=30000)

        # 上传视频
        await page.locator("div[class^='container'] input[type='file']").first.set_input_files(self.file_path)

        # 等两种发布页
        while True:
            try:
                await page.wait_for_url("**/content/publish?enter_from=publish_page", timeout=3000)
                douyin_logger.info("[+] 成功进入 version_1 发布页")
                break
            except Exception:
                try:
                    await page.wait_for_url("**/content/post/video?enter_from=publish_page", timeout=3000)
                    douyin_logger.info("[+] 成功进入 version_2 发布页")
                    break
                except Exception:
                    douyin_logger.info("  [-] 正在等待进入发布页...")
                    await asyncio.sleep(0.5)

        # 标题 + 话题
        await asyncio.sleep(0.8)
        douyin_logger.info('  [-] 正在填充标题和话题...')
        title_container = (
            page.get_by_text('作品标题').locator("..").locator("xpath=following-sibling::div[1]").locator("input")
        )
        if await title_container.count():
            await title_container.fill((self.title or "")[:30])
        else:
            # 兜底：富文本
            title_rich = page.locator(".notranslate")
            if await title_rich.count():
                await title_rich.click()
                await page.keyboard.press("Control+A")
                await page.keyboard.press("Backspace")
                await page.keyboard.type(self.title or "")
                await page.keyboard.press("Enter")

        css_selector = ".zone-container"
        for tag in (self.tags or []):
            await page.type(css_selector, "#" + tag)
            await page.press(css_selector, "Space")
        douyin_logger.info(f'  [-] 共添加 {len(self.tags)} 个话题')

        # 等上传完成
        while True:
            try:
                number = await page.locator('[class^="long-card"] div:has-text("重新上传")').count()
                if number > 0:
                    douyin_logger.success("  [-] 视频上传完毕")
                    break
                else:
                    douyin_logger.info("  [-] 正在上传视频中...")
                    await asyncio.sleep(2)
                    if await page.locator('div.progress-div > div:has-text("上传失败")').count():
                        douyin_logger.error("  [-] 发现上传出错了... 准备重试")
                        await self.handle_upload_error(page)
            except Exception:
                douyin_logger.info("  [-] 正在上传视频中...")
                await asyncio.sleep(2)

        # 封面（可选）
        await self.set_thumbnail(page, self.thumbnail_path)

        # —— 添加商品（根据 txt 是否 4 行自动决定）——
        added, reason = await self.add_product(page)
        if not added and reason == "quota_reached":
            # 需求：配额弹窗出现 → 终止发布
            await context.storage_state(path=self.account_file)  # 仍然保存最新 cookie
            douyin_logger.error("  [×] 因购物车额度限制，本次任务已停止并未发布。详见 add_product_error.png / full_page.html")
            await context.close()
            await browser.close()
            return
        elif not added and reason == "error":
            # 异常也不发布
            await context.storage_state(path=self.account_file)
            douyin_logger.error("  [×] 添加商品出现异常，本次未发布。详见 add_product_error.png / full_page.html")
            await context.close()
            await browser.close()
            return

        # 头条/西瓜联动开关（按需）
        third_part_element = '[class^="info"] > [class^="first-part"] div div.semi-switch'
        try:
            if await page.locator(third_part_element).count():
                if 'semi-switch-checked' not in await page.eval_on_selector(
                    third_part_element, 'div => div.className'
                ):
                    await page.locator(third_part_element).locator('input.semi-switch-native-control').click()
        except Exception:
            pass

        # 定时发布
        if self.publish_date != 0:
            await self.set_schedule_time_douyin(page, self.publish_date)

        # 发布
        while True:
            try:
                publish_button = page.get_by_role('button', name="发布", exact=True)
                if await publish_button.count():
                    await publish_button.click()
                await page.wait_for_url("**/content/manage**", timeout=3000)
                douyin_logger.success("  [-] 视频发布成功")
                break
            except Exception:
                douyin_logger.info("  [-] 视频正在发布中...")
                await page.screenshot(full_page=True)
                await asyncio.sleep(0.5)

        await context.storage_state(path=self.account_file)  # 保存cookie
        douyin_logger.success('  [-] cookie 更新完毕！')
        await asyncio.sleep(0.5)
        await context.close()
        await browser.close()

    async def set_thumbnail(self, page: Page, thumbnail_path: str):
        if thumbnail_path:
            await page.click('text="选择封面"')
            await page.wait_for_selector("div.semi-modal-content:visible")
            await page.click('text="设置竖封面"')
            await page.wait_for_timeout(1000)
            await page.locator(
                "div[class^='semi-upload upload'] >> input.semi-upload-hidden-input"
            ).set_input_files(thumbnail_path)
            await page.wait_for_timeout(1000)
            await page.locator(
                "div[class^='extractFooter'] button:visible:has-text('完成')"
            ).click()

    async def main(self):
        async with async_playwright() as playwright:
            await self.upload(playwright)
