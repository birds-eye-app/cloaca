# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "boto3>=1.34",
#   "python-dotenv>=1.0",
# ]
# ///
"""Parallel multipart download of the eBird raw .tar to Cloudflare R2.

Three subcommands implement a fan-out-then-assemble pipeline using S3
multipart upload as the assembly mechanism: the resulting R2 object is the
byte-exact concatenation of the parts in PartNumber order, so there is no
post-processing concatenation step.

  init      HEAD the URL, decide on shard / part assignments, create the
            multipart upload, write a manifest object to R2.
  shard     Download one shard's contiguous byte range from the URL and
            upload it as a span of parts. Each part's ETag is written to
            r2://<bucket>/parts-state/<UploadId>/<NNNNN>.json so the complete
            step can find them and retries can skip already-uploaded parts.
  complete  List parts-state, sort by PartNumber, complete the multipart
            upload, verify the final object's ContentLength, clean up state.

Inputs come from environment variables (never command-line):

  EBD_TAR_URL              source URL
  R2_ACCOUNT_ID            R2 account id
  R2_BUCKET                target bucket
  R2_ACCESS_KEY_ID         R2 S3-compat key
  R2_SECRET_ACCESS_KEY     R2 S3-compat secret
  GITHUB_OUTPUT (init only) path the GHA outputs file (set by the runner)
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from urllib.request import Request, urlopen

import boto3
from dotenv import load_dotenv


# === R2 client + URL helpers ===================================================


def make_s3():
    load_dotenv()
    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


def head_total_bytes(url: str) -> int:
    with urlopen(Request(url, method="HEAD"), timeout=30) as r:
        return int(r.headers["Content-Length"])


def write_gha_output(pairs: dict[str, str | int | bool]):
    out = os.environ.get("GITHUB_OUTPUT")
    if not out:
        print("(GITHUB_OUTPUT not set; skipping output file write)", file=sys.stderr)
        return
    with open(out, "a") as f:
        for k, v in pairs.items():
            if isinstance(v, bool):
                v = "true" if v else "false"
            f.write(f"{k}={v}\n")


# === Assignment computation ====================================================


def compute_assignments(total_bytes: int, part_size: int, total_shards: int):
    """Return (assignments, total_parts).

    assignments is a list of {shard_index, start_byte, end_byte, parts: [...]}
    where each part has {part_number, start_byte, end_byte, size}. Part numbers
    are globally sequential 1..total_parts so the multipart upload sees them
    in order regardless of which shard wrote them.
    """
    full, rem = divmod(total_bytes, part_size)
    total_parts = full + (1 if rem else 0)
    if total_parts > 10000:
        raise ValueError(
            f"{total_parts} parts exceeds S3 multipart limit of 10000 — increase part-size"
        )
    if total_shards > total_parts:
        raise ValueError(
            f"{total_shards} shards but only {total_parts} parts — reduce shards"
        )

    base, extra = divmod(total_parts, total_shards)
    assignments = []
    next_part = 1
    next_byte = 0
    for shard in range(total_shards):
        n_parts = base + (1 if shard < extra else 0)
        shard_parts = []
        for _ in range(n_parts):
            sz = min(part_size, total_bytes - next_byte)
            shard_parts.append(
                {
                    "part_number": next_part,
                    "start_byte": next_byte,
                    "end_byte": next_byte + sz - 1,
                    "size": sz,
                }
            )
            next_part += 1
            next_byte += sz
        assignments.append(
            {
                "shard_index": shard,
                "start_byte": shard_parts[0]["start_byte"],
                "end_byte": shard_parts[-1]["end_byte"],
                "parts": shard_parts,
            }
        )
    return assignments, total_parts


# === init =====================================================================


def init_cmd(args):
    s3 = make_s3()
    bucket = os.environ["R2_BUCKET"]
    url = os.environ["EBD_TAR_URL"]

    total_bytes = head_total_bytes(url)
    basename = url.rsplit("/", 1)[-1]
    if not basename.endswith(".tar"):
        raise ValueError(f"URL must end in .tar: {basename}")
    release = basename[: -len(".tar")]
    raw_key = f"raw/{basename}"

    # Skip if final object already exists at expected size
    try:
        head = s3.head_object(Bucket=bucket, Key=raw_key)
        if head["ContentLength"] == total_bytes:
            print(
                f"R2 object r2://{bucket}/{raw_key} already complete "
                f"({total_bytes:,} bytes); skipping multipart init."
            )
            write_gha_output(
                {
                    "already_complete": True,
                    "total_bytes": total_bytes,
                    "raw_key": raw_key,
                    "release": release,
                }
            )
            return
    except s3.exceptions.ClientError:
        pass

    assignments, total_parts = compute_assignments(
        total_bytes, args.part_size, args.shards
    )

    resp = s3.create_multipart_upload(Bucket=bucket, Key=raw_key)
    upload_id = resp["UploadId"]

    manifest = {
        "upload_id": upload_id,
        "raw_key": raw_key,
        "release": release,
        "total_bytes": total_bytes,
        "part_size": args.part_size,
        "total_parts": total_parts,
        "total_shards": args.shards,
        "shards": assignments,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    manifest_key = f"parts-state/{upload_id}/manifest.json"
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2).encode(),
        ContentType="application/json",
    )

    print(f"Created multipart upload {upload_id}")
    print(f"  total_bytes : {total_bytes:,}")
    print(f"  total_parts : {total_parts}")
    print(f"  part_size   : {args.part_size:,}")
    print(f"  shards      : {args.shards}")
    print(f"  manifest    : r2://{bucket}/{manifest_key}")
    for a in assignments:
        n = len(a["parts"])
        first = a["parts"][0]["part_number"]
        last = a["parts"][-1]["part_number"]
        gb = (a["end_byte"] - a["start_byte"] + 1) / 1e9
        print(
            f"  shard {a['shard_index']}: parts {first}-{last} ({n} parts, {gb:.2f} GB)"
        )

    write_gha_output(
        {
            "already_complete": False,
            "upload_id": upload_id,
            "total_bytes": total_bytes,
            "raw_key": raw_key,
            "release": release,
            "manifest_key": manifest_key,
            "shards_json": json.dumps(list(range(args.shards))),
        }
    )


# === shard ====================================================================


def _read_manifest(s3, bucket: str, manifest_key: str) -> dict:
    resp = s3.get_object(Bucket=bucket, Key=manifest_key)
    return json.loads(resp["Body"].read())


def _state_key(upload_id: str, part_number: int) -> str:
    return f"parts-state/{upload_id}/{part_number:05d}.json"


def shard_cmd(args):
    s3 = make_s3()
    bucket = os.environ["R2_BUCKET"]
    url = os.environ["EBD_TAR_URL"]

    manifest = _read_manifest(s3, bucket, args.manifest_key)
    upload_id = manifest["upload_id"]
    raw_key = manifest["raw_key"]
    my_shard = manifest["shards"][args.shard_index]

    parts = my_shard["parts"]
    first_pn = parts[0]["part_number"]
    last_pn = parts[-1]["part_number"]
    total_size = my_shard["end_byte"] - my_shard["start_byte"] + 1
    print(
        f"Shard {args.shard_index}: parts {first_pn}-{last_pn} "
        f"(bytes {my_shard['start_byte']:,}-{my_shard['end_byte']:,}, "
        f"{total_size / 1e9:.2f} GB total)"
    )

    shard_t0 = time.monotonic()
    bytes_done = 0

    for part_info in parts:
        part_num = part_info["part_number"]
        start = part_info["start_byte"]
        end = part_info["end_byte"]
        expected_size = part_info["size"]
        state_key = _state_key(upload_id, part_num)

        # Skip if already uploaded
        try:
            existing = s3.get_object(Bucket=bucket, Key=state_key)
            data = json.loads(existing["Body"].read())
            if (
                data.get("part_number") == part_num
                and data.get("size") == expected_size
            ):
                print(f"  [{part_num:>5}] already uploaded; skipping")
                bytes_done += expected_size
                continue
        except s3.exceptions.ClientError:
            pass

        # Stream-download to a temp file
        with tempfile.NamedTemporaryFile(
            prefix=f"ebd-part-{part_num:05d}-", delete=False
        ) as tmp:
            tmp_path = tmp.name
        try:
            t0 = time.monotonic()
            req = Request(url, headers={"Range": f"bytes={start}-{end}"})
            with urlopen(req, timeout=300) as resp, open(tmp_path, "wb") as out:
                shutil.copyfileobj(resp, out, length=8 << 20)
            actual_size = os.path.getsize(tmp_path)
            if actual_size != expected_size:
                raise RuntimeError(
                    f"Part {part_num}: got {actual_size} bytes, expected {expected_size}"
                )
            dl_secs = time.monotonic() - t0
            dl_mbps = actual_size / 1e6 / max(dl_secs, 1e-9)
            print(
                f"  [{part_num:>5}] downloaded {actual_size:,} B "
                f"in {dl_secs:.1f}s ({dl_mbps:.2f} MB/s)"
            )

            # Upload as a part
            t1 = time.monotonic()
            with open(tmp_path, "rb") as f:
                upload_resp = s3.upload_part(
                    Bucket=bucket,
                    Key=raw_key,
                    UploadId=upload_id,
                    PartNumber=part_num,
                    Body=f,
                    ContentLength=actual_size,
                )
            up_secs = time.monotonic() - t1
            up_mbps = actual_size / 1e6 / max(up_secs, 1e-9)
            etag = upload_resp["ETag"]
            print(
                f"  [{part_num:>5}] uploaded part in {up_secs:.1f}s "
                f"({up_mbps:.2f} MB/s) etag={etag}"
            )

            # Persist state
            state = {
                "part_number": part_num,
                "etag": etag,
                "size": actual_size,
                "shard_index": args.shard_index,
                "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            s3.put_object(
                Bucket=bucket,
                Key=state_key,
                Body=json.dumps(state).encode(),
                ContentType="application/json",
            )
            bytes_done += actual_size
        finally:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass

    elapsed = time.monotonic() - shard_t0
    avg_mbps = bytes_done / 1e6 / max(elapsed, 1e-9)
    print(
        f"Shard {args.shard_index} complete: {bytes_done:,} B in "
        f"{elapsed:.1f}s ({avg_mbps:.2f} MB/s avg)"
    )


# === complete =================================================================


def complete_cmd(args):
    s3 = make_s3()
    bucket = os.environ["R2_BUCKET"]

    manifest = _read_manifest(s3, bucket, args.manifest_key)
    upload_id = manifest["upload_id"]
    raw_key = manifest["raw_key"]
    total_parts = manifest["total_parts"]
    total_bytes = manifest["total_bytes"]

    # Gather all part-state objects
    parts: list[dict] = []
    paginator = s3.get_paginator("list_objects_v2")
    prefix = f"parts-state/{upload_id}/"
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("manifest.json"):
                continue
            resp = s3.get_object(Bucket=bucket, Key=obj["Key"])
            state = json.loads(resp["Body"].read())
            parts.append({"PartNumber": state["part_number"], "ETag": state["etag"]})

    parts.sort(key=lambda p: p["PartNumber"])
    if len(parts) != total_parts:
        present = {p["PartNumber"] for p in parts}
        missing = sorted(set(range(1, total_parts + 1)) - present)
        raise RuntimeError(
            f"Expected {total_parts} parts, found {len(parts)}. Missing: "
            f"{missing[:20]}{' ...' if len(missing) > 20 else ''}"
        )

    print(f"Completing multipart upload {upload_id} with {len(parts)} parts...")
    s3.complete_multipart_upload(
        Bucket=bucket,
        Key=raw_key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )

    head = s3.head_object(Bucket=bucket, Key=raw_key)
    if head["ContentLength"] != total_bytes:
        raise RuntimeError(
            f"Final object size {head['ContentLength']:,} != expected {total_bytes:,}"
        )
    print(
        f"✓ r2://{bucket}/{raw_key} assembled: {head['ContentLength']:,} B, "
        f"etag={head['ETag']}"
    )

    # Cleanup parts-state
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            deleted += len(keys)
    print(f"Cleaned up {deleted} parts-state object(s) under {prefix}")


# === entrypoint ===============================================================


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    init_p = sub.add_parser("init", help="Create multipart upload + manifest")
    init_p.add_argument("--shards", type=int, default=3)
    init_p.add_argument(
        "--part-size",
        type=int,
        default=5_000_000_000,
        help="Bytes per part (default 5 GB; max 5 GB per S3 spec)",
    )
    init_p.set_defaults(func=init_cmd)

    shard_p = sub.add_parser("shard", help="Download a shard's parts and upload them")
    shard_p.add_argument("--manifest-key", required=True)
    shard_p.add_argument("--shard-index", type=int, required=True)
    shard_p.set_defaults(func=shard_cmd)

    complete_p = sub.add_parser("complete", help="Complete the multipart upload")
    complete_p.add_argument("--manifest-key", required=True)
    complete_p.set_defaults(func=complete_cmd)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
