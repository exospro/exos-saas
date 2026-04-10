from __future__ import annotations

from etl.ml_auth_db_multi import get_valid_access_token


def main():
    connected_seller_id = 1
    token = get_valid_access_token(connected_seller_id)

    print("TOKEN OK")
    print(token[:20] + "..." if token else "sem token")


if __name__ == "__main__":
    main()