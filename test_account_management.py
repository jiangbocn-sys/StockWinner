#!/usr/bin/env python3
"""
账户管理功能测试
"""
import sys
import os
sys.path.append('/home/bobo/StockWinner')

from services.account_management.service import get_account_management_service
import asyncio


async def test_account_management():
    """测试账户管理功能"""
    service = get_account_management_service()

    print("=== 测试账户管理功能 ===")

    # 1. 创建账户
    print("\n1. 测试创建账户...")
    result = await service.create_account(
        name="测试账户 1",
        username="test_user1",
        password="test_pass1",
        display_name="测试账户 1 号",
        is_active=1
    )

    print(f"创建结果：{result['success']}")
    if result['success']:
        account_id = result['data']['account_id']
        print(f"账户 ID: {account_id}")
        print(f"账户名称：{result['data']['name']}")
        print(f"显示名称：{result['data']['display_name']}")
    else:
        print(f"创建失败：{result['message']}")
        # 如果已存在，继续测试其他功能
        all_accounts = await service.get_all_accounts()
        if all_accounts:
            account_id = all_accounts[0]['account_id']
            print(f"使用已存在的账户：{account_id}")
        else:
            return

    # 2. 获取账户信息
    print("\n2. 测试获取账户信息...")
    account_info = await service.get_account_by_id(account_id)
    if account_info:
        print(f"获取到账户：{account_info['name']}")
        print(f"用户名：{account_info['username']}")
        print(f"显示名称：{account_info['display_name']}")
        print(f"状态：{'激活' if account_info['is_active'] else '禁用'}")
    else:
        print("获取账户信息失败")
        return

    # 3. 更新账户信息
    print("\n3. 测试更新账户信息...")
    update_result = await service.update_account(
        account_id,
        display_name="更新后的测试账户",
        is_active=1
    )

    print(f"更新结果：{update_result['success']}")
    if update_result['success']:
        print(f"更新后显示名称：{update_result['data']['display_name']}")
    else:
        print(f"更新失败：{update_result['message']}")

    # 4. 获取所有账户
    print("\n4. 测试获取所有账户...")
    all_accounts = await service.get_all_accounts()
    print(f"共有 {len(all_accounts)} 个账户")
    for acc in all_accounts[:3]:  # 显示前 3 个
        print(f"  - {acc['account_id']}: {acc['name']} ({'激活' if acc['is_active'] else '禁用'}) - {acc['display_name']}")

    # 5. 搜索账户
    print("\n5. 测试搜索账户...")
    search_results = await service.search_accounts(is_active=1)
    print(f"激活状态的账户数量：{len(search_results)}")

    # 6. 获取统计信息
    print("\n6. 测试获取统计信息...")
    stats_result = await service.get_account_statistics()
    if stats_result['success']:
        stats = stats_result['data']
        print(f"总账户数：{stats['total_accounts']}")
        print(f"激活账户数：{stats['active_accounts']}")
        print(f"禁用账户数：{stats['inactive_accounts']}")
    else:
        print(f"获取统计信息失败：{stats_result['message']}")

    # 7. 删除账户（仅当有多个账户时）
    if len(all_accounts) > 1:
        print("\n7. 测试删除账户...")
        delete_result = await service.delete_account(account_id)
        print(f"删除结果：{delete_result['success']}")
        if delete_result['success']:
            print("账户已删除")
    else:
        print("\n7. 跳过删除测试（只有一个账户）")

    print("\n=== 账户管理功能测试完成 ===")


if __name__ == "__main__":
    asyncio.run(test_account_management())