# -*- coding: utf-8 -*-
import os, re, sys, time, socket, shutil, hashlib, platform, argparse, subprocess, locale
from pathlib import Path
from datetime import datetime, timezone, timedelta
import httpx

ENV = os.getenv

# ====== 固定配置（可用环境变量覆盖） ======
FEISHU_APP_ID     = ENV("FEISHU_APP_ID",     "cli_a8c144a13873900e")
FEISHU_APP_SECRET = ENV("FEISHU_APP_SECRET", "ziFE0ZHt56BazGPPGHUjrd4kBj64PiQL")

APP_TOKEN = ENV("FEISHU_APP_TOKEN", "AIBjbzgdWaGDqUsxaMNcPvnZnbe")
TABLE_ID  = ENV("FEISHU_TABLE_ID",  "tbl9cVL7PRUunZIc")
VIEW_ID   = ENV("FEISHU_VIEW_ID",   "veweQv45tQ")

ROOT_DIR     = Path(ENV("AUTO_FABU_ROOT", Path(__file__).resolve().parent))
VIDEOS_DIR   = Path(ENV("VIDEOS_DIR",  ROOT_DIR / "videos"))
RUNS_DIR     = Path(ENV("RUNS_DIR",    ROOT_DIR / "runs"))
COOKIES_DIR  = Path(ENV("COOKIES_DIR", ROOT_DIR / "cookies"))
PROFILES_DIR = Path(ENV("PROFILES_DIR",ROOT_DIR / "profiles"))

PYTHON_EXE = sys.executable
CLI_PATH   = str(ROOT_DIR / "cli_main.py")

TZ = timezone(timedelta(hours=8))

# ====== 表字段 ======
FIELD_WORKDIR     = "作品文件夹"         # 必须是“视频文件绝对路径”
FIELD_ACCOUNT     = "发布帐号"
FIELD_PUBTIME     = "发布时间"
FIELD_TITLE       = "标题"
FIELD_TOPICS      = "必带话题_tags"
FIELD_LINK        = "发布链接"
FIELD_SHORT_TITLE = "商品短标题"
FIELD_STATUS      = "发布状态"           # 单选（仅传“名称字符串”）
FIELD_DYID        = "抖音链接/ID"
FIELD_ERR         = "错误信息"           # 文本
FIELD_HOST        = "执行机器"
FIELD_LAST_RUN    = "最后执行时间"

# ====== 单选名称（不要再用 optXXXX） ======
STATUS_OK_NAME   = ENV("STATUS_OK_NAME",   "执行成功")
STATUS_FAIL_NAME = ENV("STATUS_FAIL_NAME", "执行失败")

VIDEO_EXTS = (".mp4", ".mov", ".mkv", ".avi", ".wmv", ".m4v")

def log(msg: str):
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} | {msg}")

def now_ms(): return int(datetime.now(TZ).timestamp() * 1000)

def to_epoch_ms(value):
    if value is None: return None
    if isinstance(value, (int, float)): return int(value)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return None

def slugify(s: str) -> str:
    s = re.sub(r"[\\/:*?\"<>|]+", " ", str(s or ""))
    s = re.sub(r"\s+", " ", s).strip()
    return s[:60] if len(s) > 60 else s

def machine_id() -> str:
    base = f"{socket.gethostname()}|{platform.system()}|{platform.machine()}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]

def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True); return p

def unique_path(p: Path) -> Path:
    if not p.exists(): return p
    stem, suf = p.stem, p.suffix
    i = 1
    while True:
        q = p.with_name(f"{stem}-{i}{suf}")
        if not q.exists(): return q
        i += 1

def normalize_topics(value: str) -> str:
    if not value: return ""
    s = value.replace("，", ",").replace("、", ",")
    s = re.sub(r"[#\s]+", ",", s)
    parts = [p for p in s.split(",") if p]
    seen, out = set(), []
    for p in parts:
        if p not in seen:
            seen.add(p); out.append(p)
    return ",".join(out)

def write_txt_for(video_dst: Path, title: str, topics: str, link: str, short_title: str):
    txt = video_dst.with_suffix(".txt")
    lines = [
        (title or "").strip(),
        normalize_topics(topics or ""),   # 第二行：必带话题_tags
        (link or "").strip(),             # 第三行：发布链接
        (short_title or "").strip(),      # 第四行：商品短标题
    ]
    txt.write_text("\n".join(lines), encoding="utf-8")
    return txt

# ========= Feishu API =========
def feishu_headers(token: str): return {"Authorization": f"Bearer {token}"}

def get_tenant_access_token() -> str:
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    payload = {"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
    with httpx.Client(timeout=20) as c:
        r = c.post(url, json=payload); r.raise_for_status()
        data = r.json(); token = data.get("tenant_access_token")
        if not token: raise RuntimeError(f"get token fail: {data}")
        return token

def list_records(token: str):
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
    params = {"page_size": 500}
    if VIEW_ID: params["view_id"] = VIEW_ID
    items = []
    with httpx.Client(timeout=30) as c:
        while True:
            r = c.get(url, headers=feishu_headers(token), params=params); r.raise_for_status()
            d = r.json().get("data", {})
            items.extend(d.get("items", []))
            if d.get("has_more") and d.get("page_token"):
                params["page_token"] = d["page_token"]; continue
            break
    return items

def _clean_record_id(rid: str) -> str:
    if not isinstance(rid, str): return ""
    rid = rid.strip()
    rid = "".join(ch for ch in rid if ch.isalnum())
    return rid

# ====== 核心：batch_update 封装 ======
def batch_update_records(token: str, records: list[dict]) -> dict:
    """
    records: [{"record_id": "recXXXX", "fields": {...}}, ...]
    返回原始JSON（code==0为成功）
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records/batch_update"
    with httpx.Client(timeout=25) as c:
        r = c.post(url, headers=feishu_headers(token), json={"records": records})
        # 不 raise_for_status，让上层能看见飞书的业务 code / msg
        return r.json()

def batch_update_one(token: str, record_id: str, fields: dict) -> bool:
    rid = _clean_record_id(record_id)
    if not (rid and rid.startswith("rec")):
        log(f"[WARN] record_id 异常，放弃回写：{record_id!r}")
        return False
    j = batch_update_records(token, [{"record_id": rid, "fields": fields}])
    if j.get("code") == 0:
        return True
    # 兼容某些 404 或跨表/删除后的失败
    log(f"[WARN] batch_update 失败：{j}")
    return False

def rescue_batch_update(token: str, fields_to_set: dict, rescue_keys: dict) -> bool:
    """
    当记录被移表/删除或 record_id 脏字符导致失败时，重扫全表并按关键字段重定位，再 batch_update。
    rescue_keys 例：{FIELD_WORKDIR: abs_video_path, FIELD_ACCOUNT: account, FIELD_PUBTIME: pub_ms}
    """
    log(f"[RESCUE] 尝试按字段重查记录回写... keys={rescue_keys}")
    try:
        items = list_records(token)
    except Exception as e2:
        log(f"[RESCUE] 拉表失败：{e2}")
        return False

    target_id = None
    for it in items:
        f = it.get("fields", {})
        ok = True
        for k, v in rescue_keys.items():
            if v is None: continue
            if f.get(k) != v: ok = False; break
        if ok:
            target_id = it.get("record_id"); break

    if not target_id:
        log("[RESCUE] 未能通过字段定位到记录，放弃回写")
        return False

    ok = batch_update_one(token, target_id, fields_to_set)
    if ok:
        log(f"[RESCUE] 已通过重查 record_id={target_id} 回写成功")
    else:
        log(f"[RESCUE] 仍失败")
    return ok

# ========= 业务 =========
def ready_to_publish(fields: dict) -> bool:
    status = fields.get(FIELD_STATUS)
    pub_ms = to_epoch_ms(fields.get(FIELD_PUBTIME))
    return (not status) and (pub_ms is not None) and (pub_ms <= now_ms())

def build_dest_name(account: str, pub_ms: int, src_video: Path) -> str:
    ts = datetime.fromtimestamp(pub_ms/1000, TZ).strftime("%Y%m%d-%H%M")
    return f"{slugify(account)}_{ts}_{src_video.stem}{src_video.suffix}"

def find_error_screenshot(account: str, start_ts: float) -> Path | None:
    acc_dir = RUNS_DIR / account
    if not acc_dir.exists(): return None
    pngs = [p for p in acc_dir.rglob("add_product_error*.png") if p.stat().st_mtime >= start_ts]
    if not pngs: return None
    pngs.sort(key=lambda p: p.stat().st_mtime)
    return pngs[-1]

def run_cli_upload(account: str, video_path: Path, publish_ts_ms: int | None, headed: bool = False) -> tuple[int, str]:
    pt_arg = "0" if (not publish_ts_ms or publish_ts_ms <= now_ms()) else str(int(publish_ts_ms/1000))
    cmd = [PYTHON_EXE, CLI_PATH, "douyin", account, "upload", str(video_path), "-pt", pt_arg]
    if headed: cmd.append("--headed")  # 透传给 CLI

    log(f"[CLI] {' '.join(cmd)}")

    enc = locale.getpreferredencoding(False)
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding=enc,
        errors="replace",
    )
    out_lines = []
    while True:
        line = proc.stdout.readline()
        if not line and proc.poll() is not None:
            break
        if line:
            print(line.rstrip())
            out_lines.append(line)
    code = proc.wait()
    return code, "".join(out_lines)

def process_one_record(token: str, rec: dict, headed: bool = False):
    rid    = rec.get("record_id")
    fields = rec.get("fields", {})

    account  = str(fields.get(FIELD_ACCOUNT) or "").strip()
    video_fp = fields.get(FIELD_WORKDIR)
    pub_ms   = to_epoch_ms(fields.get(FIELD_PUBTIME))
    title    = fields.get(FIELD_TITLE) or ""
    topics   = fields.get(FIELD_TOPICS) or ""
    link     = fields.get(FIELD_LINK) or ""
    s_title  = fields.get(FIELD_SHORT_TITLE) or ""

    host = f"{socket.gethostname()}-{machine_id()}"
    now_iso = datetime.now(TZ).isoformat(timespec="seconds")

    # 只接受“视频绝对路径”
    rescue_keys = {FIELD_WORKDIR: video_fp if isinstance(video_fp, str) else video_fp, FIELD_ACCOUNT: account, FIELD_PUBTIME: pub_ms}

    if not isinstance(video_fp, str) or (not os.path.isabs(video_fp)):
        payload = {
            FIELD_STATUS: STATUS_FAIL_NAME,          # ✅ 单选传“名称字符串”
            FIELD_ERR: "作品文件夹必须是视频文件绝对路径",
            FIELD_HOST: host, FIELD_LAST_RUN: now_iso
        }
        if not batch_update_one(token, rid, payload):
            rescue_batch_update(token, payload, rescue_keys)
        log(f"[FAIL] {rid} 非绝对路径：{video_fp}")
        return

    src_video = Path(video_fp)
    if (not src_video.exists()) or (not src_video.is_file()):
        payload = {
            FIELD_STATUS: STATUS_FAIL_NAME,
            FIELD_ERR: "视频文件不存在或不是文件",
            FIELD_HOST: host, FIELD_LAST_RUN: now_iso
        }
        if not batch_update_one(token, rid, payload):
            rescue_batch_update(token, payload, rescue_keys)
        log(f"[FAIL] {rid} 视频不存在/非文件：{video_fp}")
        return

    ensure_dir(VIDEOS_DIR)
    dest_name = build_dest_name(account, pub_ms or now_ms(), src_video)
    video_dst = unique_path(VIDEOS_DIR / dest_name)
    shutil.copy2(src_video, video_dst)
    txt_path = write_txt_for(video_dst, title, topics, link, s_title)
    log(f"[PREP] 拷贝视频到 {video_dst}，生成 {txt_path.name}")

    start_ts = time.time()
    code, output = run_cli_upload(account, video_dst, pub_ms, headed=headed)
    _ = find_error_screenshot(account, start_ts)  # 如要上传图片，可在此处读取，但现在错误信息已写文本

    # 用于救援匹配（跨表/删除导致 record_id 无效）
    src_video_str = str(src_video.resolve())
    rescue_keys = {FIELD_WORKDIR: src_video_str, FIELD_ACCOUNT: account, FIELD_PUBTIME: pub_ms}

    if code == 0 and ("视频发布成功" in output or "视频发布成功" in output.replace(" ", "")):
        payload = {
            FIELD_STATUS: STATUS_OK_NAME,            # ✅ 名称字符串
            FIELD_HOST: host, FIELD_LAST_RUN: now_iso
        }
        if not batch_update_one(token, rid, payload):
            if rescue_batch_update(token, payload, rescue_keys):
                log(f"[OK] 记录 {rid} 执行成功并已回写（经救援）")
            else:
                log(f"[WARN] 记录 {rid} 执行成功，但回写失败")
        else:
            log(f"[OK] 记录 {rid} 执行成功并已回写")
    else:
        err_text = "购物车额度已满" if "额度已满" in output else "发布失败"
        payload = {
            FIELD_STATUS: STATUS_FAIL_NAME,          # ✅ 名称字符串
            FIELD_ERR: err_text,                     # ✅ 文本
            FIELD_HOST: host, FIELD_LAST_RUN: now_iso
        }
        if not batch_update_one(token, rid, payload):
            if rescue_batch_update(token, payload, rescue_keys):
                log(f"[FAIL] 记录 {rid} 执行失败（已回写：{err_text}，经救援）")
            else:
                log(f"[FAIL] 记录 {rid} 执行失败，且回写失败")
        else:
            log(f"[FAIL] 记录 {rid} 执行失败（已回写：{err_text}）")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--headed", action="store_true", help="以可视化模式运行（转传给 cli_main.py 的 --headed）")
    return p.parse_args()

def main():
    args = parse_args()
    headed_env = os.getenv("DISPATCH_HEADED", "").strip().lower() in ("1", "true", "yes")
    headed = args.headed or headed_env

    log("Feishu Dispatcher 启动")
    # log(f"Using Feishu app={APP_TOKEN} table={TABLE_ID} view={VIEW_ID or '<default>'}")
    log(f"Browser mode: {'VISUAL (headed)' if headed else 'HEADLESS'}")

    try:
        token = get_tenant_access_token()
    except Exception as e:
        log(f"[FATAL] 获取 token 失败：{e}"); sys.exit(2)

    try:
        recs = list_records(token)
    except Exception as e:
        log(f"[FATAL] 读取表格失败：{e}"); sys.exit(3)

    ready = [r for r in recs if ready_to_publish(r.get("fields", {}))]
    log(f"共 {len(recs)} 条记录，准备执行 {len(ready)} 条到点任务")

    for r in ready:
        try:
            process_one_record(token, r, headed=headed)
        except Exception as e:
            log(f"[ERROR] 处理记录出错：{e}")

    log("Feishu Dispatcher 完成")

if __name__ == "__main__":
    main()
