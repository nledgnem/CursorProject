"""Decode just the fresh OI/liq/funding files (the big price files are blocked by MCP size limit)."""
import base64, json
from datetime import datetime, timezone
from pathlib import Path

CACHE = Path(__file__).resolve().parent / "_cache"
TOOL_RESULTS = Path(r"C:\Users\Admin\.claude\projects\C--Users-Admin-Documents-Cursor--claude-worktrees-cranky-robinson-143c9f\2eb0b7c7-761d-4130-8339-099b1582322d\tool-results")

FILES = [
    ("187EL5lcFii_Mbbz8TrTW60mtt71S0Xz7", "fact_open_interest.parquet",     "2026-06-18T02:01:48.158Z", "mcp-ae11c06b-473d-446f-92ce-c877b19171e6-download_file_content-1781831318769.txt"),
    ("1XUkiBdtxaSsUZ_-ppzftVu5Ujwr544Xj", "fact_liquidations.parquet",      "2026-06-18T02:01:48.158Z", "mcp-ae11c06b-473d-446f-92ce-c877b19171e6-download_file_content-1781831334940.txt"),
    ("1F4b0967EUW8ne1mff_h5Qwa9J5FSayCc", "silver_fact_funding.parquet",    "2026-06-18T02:01:48.158Z", "mcp-ae11c06b-473d-446f-92ce-c877b19171e6-download_file_content-1781831344938.txt"),
]

now = datetime.now(timezone.utc).isoformat()
for file_id, name, modified, saved in FILES:
    src = TOOL_RESULTS / saved
    if not src.exists():
        print(f"!! missing: {src}"); continue
    obj = json.loads(src.read_text(encoding="utf-8"))
    binary = base64.b64decode(obj["content"])
    dest = CACHE / name
    dest.write_bytes(binary)
    meta = CACHE / f"{name}.meta.json"
    meta.write_text(json.dumps({
        "file_id": file_id, "drive_modified_time": modified,
        "fetched_at": now, "size_bytes": len(binary),
    }, indent=2), encoding="utf-8")
    print(f"  {name}: {len(binary):,} bytes")
