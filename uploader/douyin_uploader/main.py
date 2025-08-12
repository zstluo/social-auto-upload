# -*- coding: utf-8 -*-
from datetime import datetime
from playwright.async_api import Playwright, async_playwright, Page
import os
import asyncio
import re

from conf import LOCAL_CHROME_PATH
from utils.base_social_media import set_init_script
from utils.log import douyin_logger


async def cookie_auth(account_file: str) -> bool:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context(storage_state=account_file)
        context = await set_init_script(context)

        page = await context.new_page()
        await page.goto("https://creator.douyin.com/creator-micro/content/upload")

        try:
            await page.wait_for_url(
                "https://creator.douyin.com/creator-micro/content/upload", timeout=5000
            )
        except Exception:
            print("[+] 等待5秒 cookie 失效")
            await context.close()
            await browser.close()
            return False

        # 2024.06.17 抖音创作者中心改版
        if await page.get_by_text('手机号登录').count() or await page.get_by_text('扫码登录').count():
            print("[+] 等待5秒 cookie 失效")
            await context.close()
            await browser.close()
            return False
        else:
            print("[+] cookie 有效")
            await context.close()
            await browser.close()
            return True


async def douyin_setup(account_file: str, handle: bool = False) -> bool:
    if not os.path.exists(account_file) or not await cookie_auth(account_file):
        if not handle:
            return False
        douyin_logger.info('[+] cookie文件不存在或已失效，即将自动打开浏览器，请扫码登录，登陆后会自动生成cookie文件')
        await douyin_cookie_gen(account_file)
    return True


async def douyin_cookie_gen(account_file: str) -> None:
    async with async_playwright() as playwright:
        options = {'headless': False}
        browser = await playwright.chromium.launch(**options)
        context = await browser.new_context()
        context = await set_init_script(context)

        page = await context.new_page()
        await page.goto("https://creator.douyin.com/")
        await page.pause()  # 点击调试器继续后，会保存 cookie
        await context.storage_state(path=account_file)

        await context.close()
        await browser.close()


class DouYinVideo(object):
    def __init__(
        self,
        title: str,
        file_path: str,
        tags,
        publish_date: datetime,
        account_file: str,
        thumbnail_path: str = None,
        product_url: str = None,
        product_title: str = None
    ):
        self.title = title
        self.file_path = file_path
        self.tags = tags
        self.publish_date = publish_date
        self.account_file = account_file
        self.date_format = '%Y年%m月%d日 %H:%M'
        self.local_executable_path = LOCAL_CHROME_PATH
        self.thumbnail_path = thumbnail_path
        self.product_url = product_url
        self.product_title = product_title

    # ---------- 关键：等待“编辑商品”弹窗 ----------
    async def _wait_product_dialog(self, page: Page):
        """
        等待“编辑商品”弹窗（抖音挂在 .semi-portal 内，用 .semi-modal-wrap）
        """
        dialog = page.locator(".semi-portal .semi-modal-wrap:has-text('编辑商品')").last
        await dialog.wait_for(state="visible", timeout=15000)
        return dialog

    # ---------- 关键：定时发布前确保无弹窗 ----------
    async def set_schedule_time_douyin(self, page: Page, publish_date: datetime):
        # 先确保没有 modal 遮挡
        try:
            await page.locator(".semi-portal .semi-modal-wrap").wait_for(state="detached", timeout=5000)
        except Exception:
            # 兜底：尝试点“完成编辑/完成/取消/关闭”把可能的弹窗关掉
            for txt in ("完成编辑", "完成", "取消", "关闭"):
                btn = page.get_by_role("button", name=txt)
                if await btn.count():
                    try:
                        await btn.first.click()
                        await page.locator(".semi-portal .semi-modal-wrap").wait_for(state="detached", timeout=5000)
                        break
                    except Exception:
                        pass

        label_element = page.locator("[class^='radio']:has-text('定时发布')")
        await label_element.click()
        await asyncio.sleep(0.2)

        publish_date_hour = publish_date.strftime("%Y-%m-%d %H:%M")
        date_box = page.locator('.semi-input[placeholder="日期和时间"]').first
        await date_box.click()
        await page.keyboard.press("Control+A")
        await page.keyboard.type(publish_date_hour)
        await page.keyboard.press("Enter")
        await asyncio.sleep(0.2)

    async def handle_upload_error(self, page: Page):
        douyin_logger.info('视频出错了，重新上传中')
        await page.locator('div.progress-div [class^="upload-btn-input"]').set_input_files(self.file_path)

    # ---------- 关键：添加商品整链 ----------
    async def add_product(self, page: Page):
        """
        扩展信息 → 添加标签：
        1) 在“添加标签”行把类型切换为【购物车】
        2) 粘贴商品链接 → 点【添加链接】
        3) 弹出“编辑商品” → 填【商品短标题】→ 点【完成编辑】→ 等弹窗关闭
        """
        if not self.product_url:
            douyin_logger.info('  [-] 未提供商品链接，跳过加商品')
            return

        douyin_logger.info('  [-] 正在添加商品...')
        try:
            # A. 锁定“扩展信息”卡片（先按你页上稳定 class，失败再退回通用选择）
            ext_card = page.locator("div:has(> .title-bu2hyo:has-text('扩展信息'))").first
            if not await ext_card.count():
                heading = page.locator("xpath=//*[normalize-space()='扩展信息']").first
                await heading.wait_for(timeout=10000)
                ext_card = heading.locator("xpath=ancestor::*[contains(@class,'semi-card')][1]").first

            await ext_card.wait_for(state='attached', timeout=8000)
            await ext_card.scroll_into_view_if_needed()
            try:
                await ext_card.wait_for(state='visible', timeout=4000)
            except Exception:
                pass

            # B. 找到“添加标签”对应的一行
            tag_row = ext_card.locator(
                "div:has(.title-dS7kae .title-content-oaqcSp:has-text('添加标签'))"
            ).first
            if not await tag_row.count():
                tag_row = ext_card.locator(
                    "xpath=.//div[contains(@class,'semi-form-field')][.//*[contains(normalize-space(),'添加标签')]]"
                ).first

            await tag_row.wait_for(state='attached', timeout=10000)
            await tag_row.scroll_into_view_if_needed()

            # C. 左侧下拉：切到【购物车】
            type_select = tag_row.locator(".semi-select").first
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

            # D. 中间输入：#douyin_creator_pc_anchor_jump 内的输入框
            input_scope = tag_row.locator("#douyin_creator_pc_anchor_jump").first
            url_input = input_scope.locator("input, textarea, .semi-input input").first
            await url_input.wait_for(state='visible', timeout=10000)
            await url_input.click()
            try:
                await page.keyboard.press("Control+A")
            except Exception:
                pass
            await page.keyboard.press("Backspace")
            await url_input.fill(self.product_url.strip())

            # 若误成“位置”出现“输入地理位置”，再强制切回购物车
            if await tag_row.get_by_text('输入地理位置', exact=False).count():
                await type_select.click()
                await option_cart.click()
                await url_input.click()
                try:
                    await page.keyboard.press("Control+A")
                except Exception:
                    pass
                await page.keyboard.press("Backspace")
                await url_input.fill(self.product_url.strip())

            # E. 右侧点击“添加链接”
            await tag_row.get_by_text("添加链接", exact=False).first.click()

            # F. 弹窗：填写“商品短标题”→ 完成编辑 → 等弹窗关闭
            dialog = await self._wait_product_dialog(page)

            short_title = dialog.locator(
                "xpath=.//*[contains(normalize-space(),'商品短标题')]/ancestor::div[contains(@class,'semi-form-field')][1]//input | "
                ".//*[contains(normalize-space(),'商品短标题')]/ancestor::div[contains(@class,'semi-form-field')][1]//textarea"
            )
            if await short_title.count() == 0:
                short_title = dialog.locator("input[placeholder*='短标题'], textarea[placeholder*='短标题']")

            await short_title.first.wait_for(state="visible", timeout=8000)
            await short_title.first.click()
            try:
                await page.keyboard.press("Control+A")
            except Exception:
                pass
            await page.keyboard.press("Backspace")
            await short_title.first.fill((self.product_title or self.title or "同款")[:10])

            finish_btn = dialog.get_by_role("button", name=re.compile("完成编辑|完成"))
            await finish_btn.first.click()
            await dialog.wait_for(state="detached", timeout=15000)

            douyin_logger.success('  [-] 商品添加完成')

        except Exception as e:
            douyin_logger.error(f'  [-] 商品添加失败: {e}')
            try:
                await page.screenshot(path='add_product_error.png', full_page=True)
            except Exception:
                pass
            try:
                with open('full_page.html', 'w', encoding='utf-8') as f:
                    f.write(await page.content())
            except Exception:
                pass

    async def upload(self, playwright: Playwright) -> None:
        # 启动浏览器
        if self.local_executable_path:
            browser = await playwright.chromium.launch(
                headless=False, executable_path=self.local_executable_path
            )
        else:
            browser = await playwright.chromium.launch(headless=False)

        # 用 cookie 创建上下文；提前授予 geolocation 权限，避免弹窗挡住点击
        context = await browser.new_context(storage_state=f"{self.account_file}")
        context = await set_init_script(context)
        await context.grant_permissions(["geolocation"], origin="https://creator.douyin.com")
        await context.set_geolocation({"latitude": 30.2741, "longitude": 120.1551})

        page = await context.new_page()
        await page.goto("https://creator.douyin.com/creator-micro/content/upload")
        douyin_logger.info(f'[+]正在上传-------{self.title}.mp4')
        douyin_logger.info('[-] 正在打开主页...')
        await page.wait_for_url("https://creator.douyin.com/creator-micro/content/upload")

        # 选择视频
        await page.locator("div[class^='container'] input").set_input_files(self.file_path)

        # 等待进入发布页面（两种版本）
        while True:
            try:
                await page.wait_for_url(
                    "https://creator.douyin.com/creator-micro/content/publish?enter_from=publish_page",
                    timeout=3000
                )
                douyin_logger.info("[+] 成功进入version_1发布页面!")
                break
            except Exception:
                try:
                    await page.wait_for_url(
                        "https://creator.douyin.com/creator-micro/content/post/video?enter_from=publish_page",
                        timeout=3000
                    )
                    douyin_logger.info("[+] 成功进入version_2发布页面!")
                    break
                except Exception:
                    print("  [-] 超时未进入视频发布页面，重新尝试...")
                    await asyncio.sleep(0.5)

        # 标题 + 话题
        await asyncio.sleep(1)
        douyin_logger.info('  [-] 正在填充标题和话题...')
        title_container = (
            page.get_by_text('作品标题')
            .locator("..")
            .locator("xpath=following-sibling::div[1]")
            .locator("input")
        )
        if await title_container.count():
            await title_container.fill(self.title[:30])
        else:
            titlecontainer = page.locator(".notranslate")
            await titlecontainer.click()
            await page.keyboard.press("Backspace")
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Delete")
            await page.keyboard.type(self.title)
            await page.keyboard.press("Enter")

        css_selector = ".zone-container"
        for tag in self.tags:
            await page.type(css_selector, "#" + tag)
            await page.press(css_selector, "Space")
        douyin_logger.info(f'总共添加{len(self.tags)}个话题')

        # 等待上传完成
        while True:
            try:
                number = await page.locator('[class^="long-card"] div:has-text("重新上传")').count()
                if number > 0:
                    douyin_logger.success("  [-]视频上传完毕")
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

        # 添加商品
        await self.add_product(page)

        # 头条/西瓜联动开关
        third_part_element = '[class^="info"] > [class^="first-part"] div div.semi-switch'
        if await page.locator(third_part_element).count():
            if 'semi-switch-checked' not in await page.eval_on_selector(
                third_part_element, 'div => div.className'
            ):
                await page.locator(third_part_element).locator('input.semi-switch-native-control').click()

        # 定时发布
        if self.publish_date != 0:
            await self.set_schedule_time_douyin(page, self.publish_date)

        # 发布
        while True:
            try:
                publish_button = page.get_by_role('button', name="发布", exact=True)
                if await publish_button.count():
                    await publish_button.click()
                await page.wait_for_url(
                    "https://creator.douyin.com/creator-micro/content/manage**",
                    timeout=3000
                )
                douyin_logger.success("  [-]视频发布成功")
                break
            except Exception:
                douyin_logger.info("  [-] 视频正在发布中...")
                await page.screenshot(full_page=True)
                await asyncio.sleep(0.5)

        await context.storage_state(path=self.account_file)  # 保存cookie
        douyin_logger.success('  [-]cookie更新完毕！')
        await asyncio.sleep(1)
        await context.close()
        await browser.close()

    async def set_thumbnail(self, page: Page, thumbnail_path: str):
        if thumbnail_path:
            await page.click('text="选择封面"')
            await page.wait_for_selector("div.semi-modal-content:visible")
            await page.click('text="设置竖封面"')
            await page.wait_for_timeout(2000)
            await page.locator(
                "div[class^='semi-upload upload'] >> input.semi-upload-hidden-input"
            ).set_input_files(thumbnail_path)
            await page.wait_for_timeout(2000)
            await page.locator(
                "div[class^='extractFooter'] button:visible:has-text('完成')"
            ).click()

    async def set_location(self, page: Page, location: str = "杭州市"):
        await page.locator('div.semi-select span:has-text("输入地理位置")').click()
        await page.keyboard.press("Backspace")
        await page.wait_for_timeout(2000)
        await page.keyboard.type(location)
        await page.wait_for_selector('div[role="listbox"] [role="option"]', timeout=5000)
        await page.locator('div[role="listbox"] [role="option"]').first.click()

    async def main(self):
        async with async_playwright() as playwright:
            await self.upload(playwright)
