from __future__ import annotations

import requests

from etl.ml_auth_db_multi import get_valid_access_token


def main():
    connected_seller_id = 1
    token = get_valid_access_token(connected_seller_id)

    headers = {
        "Authorization": f"Bearer {token}"
    }

    resp = requests.get(
        "https://api.mercadolibre.com/users/me",
        headers=headers,
        timeout=60,
    )

    print("STATUS:", resp.status_code)
    print(resp.json())


if __name__ == "__main__":
    main()