"""
账户管理 API 路由
"""
from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List
from services.account_management.service import get_account_management_service
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/accounts", tags=["accounts"])


@router.get("/statistics")
async def get_account_statistics():
    """获取账户统计信息"""
    service = get_account_management_service()
    result = await service.get_account_statistics()
    return result


@router.post("/search")
async def search_accounts(criteria: Dict[str, Any]):
    """
    搜索账户

    搜索条件:
    - is_active: 是否激活
    - name: 账户名称
    - username: 用户名
    - display_name: 显示名称
    """
    service = get_account_management_service()
    accounts = await service.search_accounts(**criteria)
    return {"success": True, "data": accounts}


@router.post("/create")
async def create_account(account_info: Dict[str, Any]):
    """
    创建新账户

    请求体参数:
    - name: 账户名称（唯一标识）
    - username: 用户名
    - password: 密码
    - display_name: 显示名称 (可选)
    - is_active: 是否激活 (1=激活，0=禁用) (可选，默认 1)
    """
    service = get_account_management_service()
    result = await service.create_account(**account_info)

    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result["message"])


@router.get("/")
async def get_all_accounts():
    """获取所有账户"""
    service = get_account_management_service()
    accounts = await service.get_all_accounts()
    return {"success": True, "data": accounts}


@router.get("/{account_id}")
async def get_account(account_id: str):
    """根据账户 ID 获取账户信息"""
    service = get_account_management_service()
    account = await service.get_account_by_id(account_id)

    if account:
        return {"success": True, "data": account}
    else:
        raise HTTPException(status_code=404, detail="账户不存在")


@router.put("/{account_id}")
async def update_account(account_id: str, updates: Dict[str, Any]):
    """
    更新账户信息

    可更新字段:
    - name: 账户名称
    - username: 用户名
    - password: 密码 (会自动哈希)
    - display_name: 显示名称
    - is_active: 是否激活
    """
    service = get_account_management_service()
    result = await service.update_account(account_id, **updates)

    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result["message"])


@router.delete("/{account_id}")
async def delete_account(account_id: str):
    """删除账户"""
    service = get_account_management_service()
    result = await service.delete_account(account_id)

    if result["success"]:
        return result
    else:
        raise HTTPException(status_code=400, detail=result["message"])
