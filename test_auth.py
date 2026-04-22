#!/usr/bin/env python3
"""
测试认证服务
"""
import asyncio
import sys
sys.path.insert(0, '/home/bobo/StockWinner')

from services.auth.service import get_auth_service
from services.common.database import get_db_manager

async def test_auth():
    """测试认证功能"""

    # 初始化数据库
    db = get_db_manager()
    await db.connect()

    # 获取认证服务
    auth = get_auth_service()

    print("=" * 50)
    print("测试认证服务")
    print("=" * 50)

    # 1. 测试登录（使用数据库中已存在的账户）
    print("\n1. 测试登录...")
    result = await auth.login("test", "test123")
    print(f"登录结果：{result}")

    if result["success"]:
        token = result["token"]
        account = result["account"]

        print(f"\n登录成功！")
        print(f"Token: {token[:20]}...")
        print(f"账户信息：{account}")

        # 2. 验证 token
        print("\n2. 验证 token...")
        validated = auth.validate_token(token)
        if validated:
            print(f"Token 有效，用户：{validated.get('username')}")
        else:
            print("Token 无效")

        # 3. 获取券商 credentials
        print("\n3. 获取券商 credentials...")
        creds = auth.get_broker_credentials(token)
        print(f"券商信息：{creds}")

        # 4. 测试登出
        print("\n4. 测试登出...")
        await auth.logout(token)
        validated_after = auth.validate_token(token)
        if not validated_after:
            print("登出成功，token 已失效")
        else:
            print("登出失败")

    else:
        print("登录失败，可能因为测试账户不存在")
        print("请先在账户管理页面创建一个测试账户")

    # 关闭数据库
    await db.close()
    print("\n" + "=" * 50)
    print("测试完成")
    print("=" * 50)

if __name__ == "__main__":
    asyncio.run(test_auth())
