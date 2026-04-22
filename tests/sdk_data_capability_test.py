"""
SDK 数据能力探测脚本

目的：
1. 探查 SDK 能提供的所有数据字段
2. 评估是否满足扩展因子的计算需求
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.common.sdk_manager import get_sdk_manager
import pandas as pd


def print_section(title: str):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def test_sdk_login():
    """测试 SDK 登录"""
    print_section("1. SDK 登录测试")
    try:
        manager = get_sdk_manager()
        info = manager.get_info()
        print("✓ SDK 登录成功")
        print(f"  InfoData 实例：{info}")
        return True
    except Exception as e:
        print(f"✗ SDK 登录失败：{e}")
        return False


def test_equity_structure():
    """测试股本结构数据"""
    print_section("2. 股本结构数据 (get_equity_structure)")
    try:
        manager = get_sdk_manager()
        test_stocks = ["689009.SH", "000001.SZ", "600000.SH"]
        df = manager.get_equity_structure(test_stocks)

        if df.empty:
            print("✗ 返回数据为空")
            return None

        print(f"✓ 获取成功，数据条数：{len(df)}")
        print(f"\n列名 ({len(df.columns)} 个):")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")

        print(f"\n示例数据 (689009.SH):")
        sample = df[df['MARKET_CODE'] == '689009.SH'].head(2)
        print(sample.to_string())

        return df
    except Exception as e:
        print(f"✗ 获取失败：{e}")
        return None


def test_income_statement():
    """测试利润表数据"""
    print_section("3. 利润表数据 (get_income_statement)")
    try:
        manager = get_sdk_manager()
        test_stocks = ["689009.SH", "000001.SZ"]
        df = manager.get_income_statement(test_stocks)

        if df.empty:
            print("✗ 返回数据为空")
            return None

        print(f"✓ 获取成功，数据条数：{len(df)}")
        print(f"\n列名 ({len(df.columns)} 个):")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")

        print(f"\n示例数据 (最新的一条):")
        print(df.iloc[-1:].to_string())

        return df
    except Exception as e:
        print(f"✗ 获取失败：{e}")
        return None


def test_balance_sheet():
    """测试资产负债表数据"""
    print_section("4. 资产负债表数据 (get_balance_sheet)")
    try:
        manager = get_sdk_manager()
        test_stocks = ["689009.SH", "000001.SZ"]
        df = manager.get_balance_sheet(test_stocks)

        if df.empty:
            print("✗ 返回数据为空")
            return None

        print(f"✓ 获取成功，数据条数：{len(df)}")
        print(f"\n列名 ({len(df.columns)} 个):")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")

        print(f"\n示例数据 (最新的一条):")
        print(df.iloc[-1:].to_string())

        return df
    except Exception as e:
        print(f"✗ 获取失败：{e}")
        return None


def test_cash_flow():
    """测试现金流量表数据"""
    print_section("5. 现金流量表数据 (get_cash_flow_statement)")
    try:
        manager = get_sdk_manager()
        test_stocks = ["689009.SH", "000001.SZ"]
        df = manager.get_cash_flow_statement(test_stocks)

        if df.empty:
            print("✗ 返回数据为空")
            return None

        print(f"✓ 获取成功，数据条数：{len(df)}")
        print(f"\n列名 ({len(df.columns)} 个):")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")

        print(f"\n示例数据 (最新的一条):")
        print(df.iloc[-1:].to_string())

        return df
    except Exception as e:
        print(f"✗ 获取失败：{e}")
        return None


def test_industry_info():
    """测试行业分类数据"""
    print_section("6. 行业分类数据 (get_industry_base_info)")
    try:
        manager = get_sdk_manager()
        df = manager.get_industry_base_info()

        if df.empty:
            print("✗ 返回数据为空")
            return None

        print(f"✓ 获取成功，数据条数：{len(df)}")
        print(f"\n列名 ({len(df.columns)} 个):")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")

        print(f"\n示例数据:")
        print(df.head(3).to_string())

        return df
    except Exception as e:
        print(f"✗ 获取失败：{e}")
        return None


def test_kline_data():
    """测试 K 线数据"""
    print_section("7. K 线数据 (MarketData.get_kline_data)")
    try:
        from AmazingData import MarketData
        market = MarketData()

        test_stocks = ["689009.SH"]
        result = market.get_kline_data(
            test_stocks,
            period="day",
            start_date=20260101,
            end_date=20260404,
            is_local=False
        )

        if not result or not result.get('689009.SH'):
            print("✗ 返回数据为空")
            return None

        df = result['689009.SH']
        print(f"✓ 获取成功，数据条数：{len(df)}")
        print(f"\n列名 ({len(df.columns)} 个):")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")

        print(f"\n示例数据:")
        print(df.tail(3).to_string())

        return df
    except Exception as e:
        print(f"✗ 获取失败：{e}")
        return None


def test_finance_indicator():
    """测试财务指标数据"""
    print_section("8. 财务指标数据 (InfoData.get_financial_indicator)")
    try:
        manager = get_sdk_manager()
        info = manager.get_info()

        test_stocks = ["689009.SH", "000001.SZ"]
        result = info.get_financial_indicator(test_stocks, is_local=False)

        if not result:
            print("✗ 返回数据为空")
            return None

        # 结果可能是 dict 或 DataFrame
        if isinstance(result, dict):
            for key, df in result.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    print(f"\n{key}:")
                    print(f"  数据条数：{len(df)}")
                    print(f"  列名：{list(df.columns)[:20]}...")
                    print(f"  示例:")
                    print(df.head(2).to_string())
        elif isinstance(result, pd.DataFrame):
            print(f"✓ 获取成功，数据条数：{len(result)}")
            print(f"\n列名 ({len(result.columns)} 个):")
            print(list(result.columns))

        return result
    except Exception as e:
        print(f"✗ 获取失败：{e}")
        return None


def test_main_stock_data():
    """测试股票基础信息"""
    print_section("9. 股票基础信息 (InfoData.get_all_stock_data)")
    try:
        manager = get_sdk_manager()
        info = manager.get_info()

        result = info.get_all_stock_data(is_local=False)

        if not result:
            print("✗ 返回数据为空")
            return None

        if isinstance(result, dict):
            for key, df in result.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    print(f"\n{key}:")
                    print(f"  数据条数：{len(df)}")
                    print(f"  列名：{list(df.columns)}")
        elif isinstance(result, pd.DataFrame):
            print(f"✓ 获取成功，数据条数：{len(result)}")
            print(f"\n列名 ({len(result.columns)} 个):")
            for i, col in enumerate(result.columns, 1):
                print(f"  {i}. {col}")

        return result
    except Exception as e:
        print(f"✗ 获取失败：{e}")
        return None


def evaluate_factor_capability():
    """评估因子计算能力"""
    print_section("📊 因子计算能力评估")

    print("""
根据 SDK 数据，评估各类因子的计算可行性：

【技术面因子】- 基于 K 线数据
├─ 均线类 (MA5/10/20/60, EMA)     → ✓ 可直接计算 (需要 close 数据)
├─ 动量类 (RSI, CCI, 动量)         → ✓ 可直接计算 (需要 close 数据)
├─ 波动类 (布林带，ATR, HV)        → ✓ 可直接计算 (需要 high/low/close)
├─ 成交量类 (OBV, 量比)            → ✓ 可直接计算 (需要 close/volume)
└─ 形态类 (金叉/死叉)              → ✓ 可直接计算 (基于 MA)

【基本面因子】- 基于财务数据
├─ 估值类 (PE_TTM, PB, PS, PCF)    → 需评估 (需要净利润/净资产/市值)
├─ 盈利类 (ROE, ROA, 毛利率)        → 需评估 (需要利润表数据)
└─ 成长类 (营收增长率，净利润增长率) → 需评估 (需要多期财务数据)

【特色因子】- A 股特有
├─ 涨停统计 (N 日涨停次数)          → ✓ 可直接计算 (需要 change_pct)
├─ 连板统计 (连续涨停)              → ✓ 可直接计算 (需要 change_pct)
├─ 异动统计 (大涨/大跌次数)         → ✓ 可直接计算 (需要 change_pct)
└─ 筹码类 (距新高/新低距离)         → ✓ 可直接计算 (需要 high/close)

【需要额外数据源的因子】
├─ 资金流向 (主力净流入)            → ✗ 需要 Level2 资金数据
├─ 北向资金持股                    → ✗ 需要沪深股通数据
├─ 分析师评级                      → ✗ 需要研报数据
└─ 新闻舆情                        → ✗ 需要新闻数据
""")


def main():
    print_section("🔍 SDK 数据能力探测脚本")
    print("目的：评估 SDK 数据是否满足扩展因子的计算需求")

    results = {}

    # 1. SDK 登录
    results['login'] = test_sdk_login()
    if not results['login']:
        print("\n⚠️  SDK 登录失败，无法继续测试")
        return

    # 2. K 线数据 (技术因子基础)
    results['kline'] = test_kline_data()

    # 3. 股本结构
    results['equity'] = test_equity_structure()

    # 4. 利润表
    results['income'] = test_income_statement()

    # 5. 资产负债表
    results['balance'] = test_balance_sheet()

    # 6. 现金流
    results['cashflow'] = test_cash_flow()

    # 7. 行业分类
    results['industry'] = test_industry_info()

    # 8. 财务指标
    results['finance_indicator'] = test_finance_indicator()

    # 9. 股票基础信息
    results['stock_base'] = test_main_stock_data()

    # 10. 评估
    evaluate_factor_capability()

    # 总结
    print_section("✅ 测试完成总结")
    print(f"SDK 登录：{'✓' if results.get('login') else '✗'}")
    print(f"K 线数据：{'✓' if results.get('kline') is not None else '✗'}")
    print(f"股本结构：{'✓' if results.get('equity') is not None else '✗'}")
    print(f"利润表：{'✓' if results.get('income') is not None else '✗'}")
    print(f"资产负债表：{'✓' if results.get('balance') is not None else '✗'}")
    print(f"现金流：{'✓' if results.get('cashflow') is not None else '✗'}")
    print(f"行业分类：{'✓' if results.get('industry') is not None else '✗'}")
    print(f"财务指标：{'✓' if results.get('finance_indicator') is not None else '✗'}")
    print(f"股票基础信息：{'✓' if results.get('stock_base') is not None else '✗'}")


if __name__ == "__main__":
    main()
