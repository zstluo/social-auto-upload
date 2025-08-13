# -*- coding: utf-8 -*-
import argparse
import asyncio
import os
from pathlib import Path
from datetime import datetime
from typing import List, Optional

from utils.log import douyin_logger as logger

try:
    from conf import BASE_DIR
except Exception:
    BASE_DIR = Path(__file__).resolve().parent

from uploader.douyin_uploader.main import (
    douyin_setup,
    DouYinVideo,
)

# 读取 .txt 元数据（可选）
def load_meta_from_txt(txt_path: Path):
    """
    .txt 四行格式：
    第一行：标题
    第二行：话题（逗号分隔）
    第三行：商品链接
    第四行：商品短标题
    """
    title = None
    tags: List[str] = []
    product_url = None
    product_title = None

    if not txt_path.exists():
        return title, tags, product_url, product_title

    lines = [line.strip() for line in txt_path.read_text(encoding="utf-8").splitlines()]
    if len(lines) >= 1 and lines[0]:
        title = lines[0]
    if len(lines) >= 2 and lines[1]:
        tags = [t.strip() for t in lines[1].replace("，", ",").split(",") if t.strip()]
    if len(lines) >= 3 and lines[2]:
        product_url = lines[2]
    if len(lines) >= 4 and lines[3]:
        product_title = lines[3]
    return title, tags, product_url, product_title


def build_cookie_path(alias: str) -> Path:
    # 强隔离：每个账号一个独立 cookie 文件
    return Path(BASE_DIR) / "cookies" / f"douyin_{alias}.json"


def parse_publish_time(pt: str | int | None) -> int | datetime:
    """
    - 0 / "0": 立即发布
    - "2025-08-12 16:30": 指定时间
    """
    if pt is None:
        return 0
    if str(pt).strip() == "0":
        return 0
    # 允许 "YYYY-MM-DD HH:MM"
    return datetime.strptime(str(pt).strip(), "%Y-%m-%d %H:%M")


def bool_from_env(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v == "1" or v.lower() in ("true", "yes", "y", "on")


async def main():
    parser = argparse.ArgumentParser(prog="cli_main.py")
    subparsers = parser.add_subparsers(dest="platform", required=True)

    # douyin 子命令
    p_dy = subparsers.add_parser("douyin", help="Douyin (抖音)")
    p_dy.add_argument("account_alias", help="账号别名（用于区分不同账号的 cookie 文件）")
    p_dy_sub = p_dy.add_subparsers(dest="action", required=True)

    # setup
    p_setup = p_dy_sub.add_parser("setup", help="打开浏览器扫码，生成/更新 cookie")
    p_setup.add_argument("--headed", action="store_true", help="以可视化模式进行 cookie 检查（默认无头）")

    # upload
    p_upload = p_dy_sub.add_parser("upload", help="上传单个视频")
    p_upload.add_argument("video", help="视频文件路径")
    p_upload.add_argument("-t", "--title", help="视频标题")
    p_upload.add_argument("-tags", "--tags", help="话题，逗号分隔，如：旅游,美食", default=None)
    p_upload.add_argument("-pt", "--publish_time", help="发布时间：0=立即；或形如 2025-08-12 16:30", default="0")
    p_upload.add_argument("--product-url", help="商品链接", default=None)
    p_upload.add_argument("--product-title", help="商品短标题（≤10字）", default=None)
    p_upload.add_argument("--thumbnail", help="封面图片路径（可选）", default=None)
    p_upload.add_argument("--headed", action="store_true", help="以可视化模式运行（默认无头）")
    p_upload.add_argument("--skip-cookie-check", action="store_true", help="跳过启动前 Cookie 校验（默认不跳过）")
    p_upload.add_argument("--meta", help="同名 .txt 元数据路径（标题/话题/商品链接/商品短标题）", default=None)

    args = parser.parse_args()

    if args.platform == "douyin":
        account_alias: str = args.account_alias
        cookie_file = build_cookie_path(account_alias)
        cookie_file.parent.mkdir(parents=True, exist_ok=True)

        if args.action == "setup":
            # 仅做 cookie 生成/校验
            headless = not args.headed
            ok = await douyin_setup(str(cookie_file), handle=True, account_alias=account_alias, headless=headless)
            if ok:
                logger.info("[cookie] cookie 有效")
            else:
                logger.info("[cookie] cookie 无效，请使用 setup 进行扫码")
            return

        if args.action == "upload":
            # 环境变量与命令行组合（命令行优先）
            headless_env = bool_from_env("HEADLESS", True)
            headless = headless_env and (not args.headed)

            # 启动前 Cookie 轻量校验（默认开启，可用 --skip-cookie-check 跳过）
            skip_check = args.skip_cookie_check or os.getenv("SKIP_COOKIE_CHECK") == "1"
            if not skip_check:
                await douyin_setup(str(cookie_file), handle=True, account_alias=account_alias, headless=headless)
            else:
                logger.info("[cookie] 跳过启动前校验（--skip-cookie-check / SKIP_COOKIE_CHECK=1）")

            # 解析视频与元数据
            video_path = Path(args.video).resolve()
            if not video_path.exists():
                raise FileNotFoundError(f"视频不存在: {video_path}")

            # .txt 元数据（优先）
            meta_title = meta_tags = meta_url = meta_p_title = None
            if args.meta:
                meta_title, meta_tags, meta_url, meta_p_title = load_meta_from_txt(Path(args.meta))
            else:
                # 默认找同名 .txt
                default_meta = video_path.with_suffix(".txt")
                if default_meta.exists():
                    meta_title, meta_tags, meta_url, meta_p_title = load_meta_from_txt(default_meta)

            # 标题与标签合并优先级：CLI > .txt > 默认
            title = args.title or meta_title or video_path.stem
            if args.tags:
                tags = [t.strip() for t in args.tags.replace("，", ",").split(",") if t.strip()]
            else:
                tags = meta_tags or []

            product_url = args.product_url or meta_url
            product_title = args.product_title or meta_p_title

            publish_time = parse_publish_time(args.publish_time)

            uploader = DouYinVideo(
                title=title,
                file_path=str(video_path),
                tags=tags,
                publish_date=publish_time,
                account_file=str(cookie_file),
                thumbnail_path=args.thumbnail,
                product_url=product_url,
                product_title=product_title,
                headless=headless,
            )

            await uploader.main()
            return


if __name__ == "__main__":
    asyncio.run(main())
