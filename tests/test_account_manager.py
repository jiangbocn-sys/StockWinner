"""
账户管理器测试
"""

import pytest
from pathlib import Path
import json
import tempfile
import shutil

from services.common.account_manager import (
    AccountManager,
    Account,
    get_account_manager,
    reset_account_manager
)


class TestAccountManager:
    """账户管理器测试"""

    @pytest.fixture
    def temp_config(self):
        """创建临时配置文件"""
        temp_dir = tempfile.mkdtemp()
        config_path = Path(temp_dir) / "accounts.json"

        config_data = {
            "test1": {
                "username": "test_user1",
                "password": "test_pass1",
                "display_name": "测试账户 1",
                "is_active": True
            },
            "test2": {
                "username": "test_user2",
                "password": "test_pass2",
                "display_name": "测试账户 2",
                "is_active": True
            },
            "inactive": {
                "username": "inactive_user",
                "password": "inactive_pass",
                "display_name": "非激活账户",
                "is_active": False
            }
        }

        with open(config_path, 'w') as f:
            json.dump(config_data, f)

        yield config_path

        shutil.rmtree(temp_dir)

    def test_load_accounts(self, temp_config, monkeypatch):
        """测试加载账户"""
        # 重置单例
        reset_account_manager()
        monkeypatch.setattr('services.common.account_manager.CONFIG_PATH', temp_config)

        manager = AccountManager()
        accounts = manager.list_accounts()

        assert len(accounts) == 3
        assert manager.get_account('test1').display_name == "测试账户 1"
        assert manager.get_account('test2').username == "test_user2"

    def test_validate_account(self, temp_config, monkeypatch):
        """测试账户验证"""
        reset_account_manager()
        monkeypatch.setattr('services.common.account_manager.CONFIG_PATH', temp_config)

        manager = AccountManager()

        # 激活账户
        assert manager.validate_account('test1') is True
        assert manager.validate_account('test2') is True

        # 非激活账户
        assert manager.validate_account('inactive') is False

        # 不存在账户
        assert manager.validate_account('nonexistent') is False

    def test_get_active_accounts(self, temp_config, monkeypatch):
        """测试获取激活账户"""
        reset_account_manager()
        monkeypatch.setattr('services.common.account_manager.CONFIG_PATH', temp_config)

        manager = AccountManager()
        active = manager.get_active_accounts()

        assert len(active) == 2
        account_ids = [acc.account_id for acc in active]
        assert 'test1' in account_ids
        assert 'test2' in account_ids
        assert 'inactive' not in account_ids

    def test_get_account_display_name(self, temp_config, monkeypatch):
        """测试获取显示名称"""
        reset_account_manager()
        monkeypatch.setattr('services.common.account_manager.CONFIG_PATH', temp_config)

        manager = AccountManager()

        assert manager.get_account_display_name('test1') == "测试账户 1"
        assert manager.get_account_display_name('nonexistent') == "nonexistent"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
