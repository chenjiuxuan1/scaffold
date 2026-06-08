#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from gateway.main import execute_request
from gateway.models import GatewayRequest
from gateway.response import build_response
from gateway.utils import decode_payload_b64


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DS scheduler gateway entry")
    parser.add_argument("--country", required=True)
    parser.add_argument("--action", required=True)
    parser.add_argument("--ds-token", required=True)
    parser.add_argument("--request-id", default="")
    parser.add_argument("--payload-b64", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        request = GatewayRequest(
            country=args.country.strip().lower(),
            action=args.action.strip(),
            ds_token=args.ds_token.strip(),
            request_id=args.request_id.strip(),
            payload=decode_payload_b64(args.payload_b64),
        )
        print(json.dumps(execute_request(request), ensure_ascii=False))
    except Exception as exc:
        print(
            json.dumps(
                build_response(
                    False,
                    args.country.strip().lower(),
                    args.action.strip(),
                    args.request_id.strip(),
                    data=None,
                    error={"code": "GATEWAY_ERROR", "message": repr(exc)},
                ),
                ensure_ascii=False,
            )
        )
        raise


if __name__ == "__main__":
    main()
