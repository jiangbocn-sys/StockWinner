"""
Common data explorer API

Provides database list, table list, table statistics, schema, data query and edit functions
"""

from fastapi import APIRouter, HTTPException, Query, Body, Path as FastAPIPath
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, timezone, timedelta
import aiosqlite
from pathlib import Path
from pydantic import BaseModel, Field

# 数据库路径
DATA_DIR = Path(__file__).parent.parent.parent / "data"
KLINE_DB_PATH = DATA_DIR / "kline.db"
STOCKWINNER_DB_PATH = DATA_DIR / "stockwinner.db"

# 支持的数据库列表
DATABASES = {
    "kline": KLINE_DB_PATH,
    "stockwinner": STOCKWINNER_DB_PATH
}

# 筛选模板配置文件
TEMPLATES_FILE = Path(__file__).parent.parent.parent / "config" / "screening_templates.json"

# 中国时区
CHINA_TZ = timezone(timedelta(hours=8))

# 导入公共股票代码工具
from services.common.stock_code import normalize_stock_code

def get_china_time():
    """获取中国时区时间"""
    return datetime.now(CHINA_TZ).replace(tzinfo=None)


from contextlib import asynccontextmanager

@asynccontextmanager
async def get_db_connection(db_path: Path):
    """
    获取带正确配置的数据库连接（上下文管理器）

    配置：
    - timeout=60秒：允许等待锁释放
    - WAL模式：减少读写锁竞争
    - busy_timeout=60000：SQLite内部等待时间
    """
    db = await aiosqlite.connect(str(db_path), timeout=60)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA busy_timeout=60000")
    await db.execute("PRAGMA synchronous=NORMAL")
    db.row_factory = aiosqlite.Row
    try:
        yield db
    finally:
        await db.close()


# ==================== Pydantic 模型 ====================

class FilterCondition(BaseModel):
    """筛选条件模型"""
    field: str = Field(..., description="字段名")
    operator: str = Field(..., description="操作符：eq, ne, gt, gte, lt, lte, in, not_in, between, like, is_null, is_not_null")
    value: Optional[Any] = Field(None, description="筛选值（is_null/is_not_null 除外）")
    value2: Optional[Any] = Field(None, description="第二个值（仅 between 操作符使用）")


class SortOption(BaseModel):
    """排序选项模型"""
    field: str = Field(..., description="字段名")
    order: str = Field("desc", description="排序方向：asc 或 desc")


class QueryRequest(BaseModel):
    """高级筛选请求模型"""
    filters: List[FilterCondition] = Field(default_factory=list, description="筛选条件列表")
    fields: Optional[List[str]] = Field(None, description="返回字段列表（默认返回所有字段）")
    sort: Optional[List[SortOption]] = Field(None, description="排序配置列表")
    limit: int = Field(100, description="返回条数", ge=1, le=10000)
    offset: int = Field(0, description="偏移量")


class QueryResponse(BaseModel):
    """高级筛选响应模型"""
    success: bool
    database: str
    table: str
    columns: List[str]
    data: List[Dict[str, Any]]
    pagination: Dict[str, Any]
    query_info: Dict[str, Any]


class AggregateRequest(BaseModel):
    """聚合统计请求模型"""
    group_by: Optional[List[str]] = Field(None, description="分组字段列表")
    aggregations: List[Dict[str, Any]] = Field(..., description="聚合配置 [{field: 'name', agg: 'sum'}]")
    filters: Optional[List[FilterCondition]] = Field(default_factory=list, description="筛选条件")


router = APIRouter()


# ==================== 筛选模板 API ====================

@router.get("/api/v1/ui/screening/templates")
async def get_screening_templates(category: Optional[str] = Query(None, description="Template category filter")):
    """Get preset screening template list."""
    if not TEMPLATES_FILE.exists():
        return {"success": True, "templates": [], "message": "模板配置文件不存在"}

    import json
    try:
        with open(TEMPLATES_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)

        templates = config.get("templates", [])

        # 按分类筛选
        if category:
            templates = [t for t in templates if t.get("category") == category]

        # 返回简化信息（不包含完整 filters）
        template_list = []
        for t in templates:
            template_list.append({
                "id": t.get("id"),
                "name": t.get("name"),
                "description": t.get("description"),
                "category": t.get("category"),
                "filters_count": len(t.get("filters", [])),
                "default_limit": t.get("default_limit", 100)
            })

        return {"success": True, "templates": template_list}

    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/api/v1/ui/screening/templates/{template_id}")
async def get_screening_template(template_id: str = FastAPIPath(..., description="模板 ID")):
    """获取单个模板的完整配置"""
    if not TEMPLATES_FILE.exists():
        raise HTTPException(status_code=404, detail="模板配置文件不存在")

    import json
    try:
        with open(TEMPLATES_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)

        templates = config.get("templates", [])
        template = next((t for t in templates if t.get("id") == template_id), None)

        if not template:
            raise HTTPException(status_code=404, detail=f"模板不存在：{template_id}")

        return {"success": True, "template": template}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/v1/ui/screening/template/{template_id}")
async def apply_screening_template(
    template_id: str = FastAPIPath(..., description="模板 ID"),
    db_name: str = Body(..., description="数据库名称"),
    table_name: str = Body(..., description="表名"),
    limit: Optional[int] = Body(None, description="覆盖默认返回条数"),
    offset: int = Body(0, description="偏移量")
):
    """
    应用筛选模板到指定数据表

    返回应用该模板后的查询结果
    """
    if not TEMPLATES_FILE.exists():
        raise HTTPException(status_code=404, detail="模板配置文件不存在")

    if db_name not in DATABASES:
        raise HTTPException(status_code=404, detail=f"数据库不存在：{db_name}")

    db_path = DATABASES[db_name]
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"数据库文件不存在：{db_name}")

    import json
    try:
        with open(TEMPLATES_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)

        templates = config.get("templates", [])
        template = next((t for t in templates if t.get("id") == template_id), None)

        if not template:
            raise HTTPException(status_code=404, detail=f"模板不存在：{template_id}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取模板失败：{str(e)}")

    # 获取模板配置
    filters = template.get("filters", [])
    sort_config = template.get("sort", [])
    default_limit = template.get("default_limit", 100)
    actual_limit = limit if limit is not None else default_limit

    async with get_db_connection(db_path) as db:
        db.row_factory = aiosqlite.Row

        # 检查表是否存在
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        if not await cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"表不存在：{table_name}")

        # 获取列信息
        cursor = await db.execute(f"PRAGMA table_info({table_name})")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]

        # 构建 WHERE 条件
        conditions = []
        params = []

        for filter_cond in filters:
            field = filter_cond.get("field")
            op = filter_cond.get("operator", "eq").lower()
            value = filter_cond.get("value")
            value2 = filter_cond.get("value2")

            if field not in column_names:
                continue  # 跳过不存在的字段

            if op == 'eq':
                conditions.append(f"{field} = ?")
                params.append(value)
            elif op == 'ne':
                conditions.append(f"{field} != ?")
                params.append(value)
            elif op == 'gt':
                conditions.append(f"{field} > ?")
                params.append(value)
            elif op == 'gte':
                conditions.append(f"{field} >= ?")
                params.append(value)
            elif op == 'lt':
                conditions.append(f"{field} < ?")
                params.append(value)
            elif op == 'lte':
                conditions.append(f"{field} <= ?")
                params.append(value)
            elif op == 'in':
                if isinstance(value, list):
                    placeholders = ','.join(['?' for _ in value])
                    conditions.append(f"{field} IN ({placeholders})")
                    params.extend(value)
            elif op == 'between':
                conditions.append(f"{field} BETWEEN ? AND ?")
                params.append(value)
                params.append(value2)
            elif op == 'like':
                conditions.append(f"{field} LIKE ?")
                params.append(f"%{value}%")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # 排序
        order_clause = ""
        if sort_config:
            order_parts = []
            for sort_opt in sort_config:
                field = sort_opt.get("field")
                order = sort_opt.get("order", "desc")
                if field in column_names:
                    order_dir = "DESC" if order.lower() == "desc" else "ASC"
                    order_parts.append(f"{field} {order_dir}")
            if order_parts:
                order_clause = " ORDER BY " + ", ".join(order_parts)

        # 执行查询
        query = f"SELECT * FROM {table_name} WHERE {where_clause}{order_clause} LIMIT ? OFFSET ?"
        params.extend([actual_limit, offset])

        cursor = await db.execute(query, tuple(params))
        rows = await cursor.fetchall()
        data = [dict(row) for row in rows]

        # 查询总数
        count_params = tuple(params[:-2]) if len(params) >= 2 else ()
        cursor = await db.execute(f"SELECT COUNT(*) as total FROM {table_name} WHERE {where_clause}", count_params)
        result = await cursor.fetchone()
        total = result[0] if result else 0

    return {
        "success": True,
        "template_applied": template_id,
        "template_name": template.get("name"),
        "database": db_name,
        "table": table_name,
        "data": data,
        "pagination": {
            "limit": actual_limit,
            "offset": offset,
            "total": total,
            "has_more": (offset + actual_limit) < total
        },
        "query_info": {
            "filters_applied": len(conditions),
            "template_description": template.get("description")
        }
    }


# ==================== 股票基本信息 API ====================

@router.get("/api/v1/ui/stocks")
async def get_stock_list(
    industry: Optional[str] = Query(None, description="行业分类筛选"),
    search: Optional[str] = Query(None, description="股票代码或名称模糊搜索"),
    limit: int = Query(100, description="返回条数", ge=1, le=1000),
    db_name: str = Query("kline", description="数据库名称")
):
    """
    获取股票列表（从 stock_monthly_factors 表获取基本信息）

    返回全市场股票列表，支持按行业筛选和模糊搜索
    """
    if db_name not in DATABASES:
        raise HTTPException(status_code=404, detail=f"数据库不存在：{db_name}")

    db_path = DATABASES[db_name]
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"数据库文件不存在：{db_name}")

    async with get_db_connection(db_path) as db:
        db.row_factory = aiosqlite.Row

        # 检查 stock_monthly_factors 表是否存在
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_monthly_factors'"
        )
        if not await cursor.fetchone():
            raise HTTPException(status_code=404, detail="stock_monthly_factors 表不存在")

        # 构建查询条件
        conditions = []
        params = []

        if industry:
            conditions.append("sw_level1 = ?")
            params.append(industry)

        if search:
            conditions.append("(stock_code LIKE ? OR stock_name LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # 获取最新报告期的股票信息（每个股票只返回一条最新记录）
        query = f"""
            SELECT DISTINCT stock_code, stock_name, sw_level1, sw_level2, sw_level3,
                   report_quarter, report_year
            FROM stock_monthly_factors
            WHERE {where_clause}
            ORDER BY stock_code
            LIMIT ?
        """
        params.append(limit)

        cursor = await db.execute(query, tuple(params))
        rows = await cursor.fetchall()
        stocks = [dict(row) for row in rows]

        # 获取行业列表
        cursor = await db.execute(
            "SELECT DISTINCT sw_level1 FROM stock_monthly_factors WHERE sw_level1 IS NOT NULL AND sw_level1 != '' ORDER BY sw_level1"
        )
        industries = [row[0] for row in await cursor.fetchall()]

    return {
        "success": True,
        "stocks": stocks,
        "industries": industries,
        "total": len(stocks)
    }


@router.get("/api/v1/ui/stocks/{stock_code}")
async def get_stock_detail(
    stock_code: str = FastAPIPath(..., description="Stock code"),
    db_name: str = Query("kline", description="Database name")
):
    """Get basic information for a single stock."""


# ==================== SDK Stock List API ====================

@router.get("/api/v1/ui/stocks/sdk/code_list")
async def get_sdk_code_list(
    security_type: str = Query("EXTRA_STOCK_A", description="Security type: EXTRA_STOCK_A=All A-shares, EXTRA_STOCK_SH=Shanghai A-shares, EXTRA_STOCK_SZ=Shenzhen A-shares")
):
    """Get latest stock list from AmazingData SDK."""
    try:
        from services.common.sdk_manager import get_sdk_manager
        import aiosqlite

        sdk_manager = get_sdk_manager()

        # Ensure SDK is logged in
        sdk_manager._ensure_login()

        # Get stock list from SDK
        stock_list = sdk_manager.get_code_list(security_type=security_type)

        # Build stock name cache from database
        db_path = KLINE_DB_PATH
        stock_names = {}
        if db_path.exists():
            async with get_db_connection(db_path) as db:
                cursor = await db.execute(
                    "SELECT DISTINCT stock_code, stock_name FROM kline_data WHERE stock_name IS NOT NULL AND stock_name != ''"
                )
                rows = await cursor.fetchall()
                stock_names = {row[0]: row[1] for row in rows}

        # Convert to readable format
        stocks = []
        for item in stock_list:
            if isinstance(item, dict):
                code = item.get("code", "")
                market = item.get("market", "")
                full_code = f"{code}.{market}" if code and market else code
                stocks.append({
                    "code": code,
                    "name": item.get("name", "") or stock_names.get(full_code, ""),
                    "market": market,
                    "full_code": full_code
                })
            elif isinstance(item, str):
                # If string only, extract market from code
                code = item
                market = ""
                name = ""
                if "." in code:
                    parts = code.split(".")
                    code = parts[0]
                    market = parts[1] if len(parts) > 1 else ""
                    # Try to get name from cache
                    name = stock_names.get(item, "")
                stocks.append({"code": code, "name": name, "market": market, "full_code": item})

        return {
            "success": True,
            "security_type": security_type,
            "stocks": stocks,
            "total": len(stocks)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取股票清单失败：{str(e)}")


@router.get("/api/v1/ui/stocks/{stock_code}")
async def get_stock_detail(
    stock_code: str = FastAPIPath(..., description="股票代码"),
    db_name: str = Query("kline", description="数据库名称")
):
    """
    Get basic information for a single stock

    Returns stock industry classification and latest market cap
    """
    if db_name not in DATABASES:
        raise HTTPException(status_code=404, detail=f"数据库不存在：{db_name}")

    db_path = DATABASES[db_name]
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"数据库文件不存在：{db_name}")

    async with get_db_connection(db_path) as db:
        db.row_factory = aiosqlite.Row

        # 从 stock_monthly_factors 获取行业分类信息
        cursor = await db.execute("""
            SELECT stock_code, stock_name, sw_level1, sw_level2, sw_level3,
                   report_quarter, report_year
            FROM stock_monthly_factors
            WHERE stock_code = ?
            ORDER BY report_date DESC
            LIMIT 1
        """, (stock_code,))
        row = await cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"股票不存在：{stock_code}")

        stock_info = dict(row)

        # 从 stock_daily_factors 获取最新市值数据
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_daily_factors'"
        )
        if await cursor.fetchone():
            cursor = await db.execute("""
                SELECT trade_date, circ_market_cap, total_market_cap,
                       days_since_ipo, change_10d, change_20d
                FROM stock_daily_factors
                WHERE stock_code = ?
                ORDER BY trade_date DESC
                LIMIT 1
            """, (stock_code,))
            market_row = await cursor.fetchone()
            if market_row:
                stock_info["market_data"] = dict(market_row)

    return {
        "success": True,
        "stock": stock_info
    }


# ==================== 数据新鲜度 API ====================

@router.get("/api/v1/ui/data/freshness")
async def get_data_freshness(db_name: str = Query("kline", description="数据库名称")):
    """
    获取数据新鲜度检查

    返回各主要数据表的最新日期和更新时间
    """
    if db_name not in DATABASES:
        raise HTTPException(status_code=404, detail=f"数据库不存在：{db_name}")

    db_path = DATABASES[db_name]
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"数据库文件不存在：{db_name}")

    freshness_info = {}

    async with get_db_connection(db_path) as db:
        db.row_factory = aiosqlite.Row

        # 检查 kline_data 表
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='kline_data'"
        )
        if await cursor.fetchone():
            cursor = await db.execute("SELECT MAX(trade_date) as latest FROM kline_data")
            row = await cursor.fetchone()
            freshness_info["kline_data"] = {
                "latest_date": row[0] if row else None,
                "description": "K 线行情数据"
            }

        # 检查 stock_daily_factors 表
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_daily_factors'"
        )
        if await cursor.fetchone():
            cursor = await db.execute("SELECT MAX(trade_date) as latest, COUNT(*) as total FROM stock_daily_factors")
            row = await cursor.fetchone()
            freshness_info["stock_daily_factors"] = {
                "latest_date": row[0] if row else None,
                "total_records": row[1] if row else 0,
                "description": "日频因子数据"
            }

        # 检查 stock_monthly_factors 表
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_monthly_factors'"
        )
        if await cursor.fetchone():
            cursor = await db.execute("SELECT MAX(report_date) as latest, COUNT(*) as total FROM stock_monthly_factors")
            row = await cursor.fetchone()
            freshness_info["stock_monthly_factors"] = {
                "latest_date": row[0] if row else None,
                "total_records": row[1] if row else 0,
                "description": "月频因子数据"
            }

    return {
        "success": True,
        "database": db_name,
        "freshness": freshness_info,
        "check_time": get_china_time().isoformat()
    }


# ==================== 数据导出 API ====================

@router.post("/api/v1/ui/databases/{db_name}/tables/{table_name}/export")
async def export_table_data(
    db_name: str = FastAPIPath(..., description="数据库名称"),
    table_name: str = FastAPIPath(..., description="表名"),
    format: str = Body("csv", description="导出格式：csv 或 json"),
    filters: Optional[List[Dict[str, Any]]] = Body(None, description="筛选条件"),
    limit: int = Body(10000, description="最大导出条数", ge=1, le=100000)
):
    """
    导出数据表

    支持 CSV 和 JSON 格式导出
    """
    if db_name not in DATABASES:
        raise HTTPException(status_code=404, detail=f"数据库不存在：{db_name}")

    db_path = DATABASES[db_name]
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"数据库文件不存在：{db_name}")

    if format not in ["csv", "json"]:
        raise HTTPException(status_code=400, detail=f"不支持的导出格式：{format}")

    async with get_db_connection(db_path) as db:
        db.row_factory = aiosqlite.Row

        # 检查表是否存在
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        if not await cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"表不存在：{table_name}")

        # 获取列信息
        cursor = await db.execute(f"PRAGMA table_info({table_name})")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]

        # 简单查询全部数据（带 limit）
        query = f"SELECT * FROM {table_name} LIMIT ?"

        cursor = await db.execute(query, (limit,))
        rows = await cursor.fetchall()
        data = [dict(row) for row in rows]

    if format == "json":
        import json
        return {
            "success": True,
            "database": db_name,
            "table": table_name,
            "format": "json",
            "records_exported": len(data),
            "data": data
        }
    else:  # csv
        import csv
        import io

        output = io.StringIO()
        if data:
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

        return {
            "success": True,
            "database": db_name,
            "table": table_name,
            "format": "csv",
            "records_exported": len(data),
            "csv_content": output.getvalue()
        }


@router.get("/api/v1/ui/databases")
async def list_databases():
    """获取可用的数据库列表"""
    db_list = []
    for name, path in DATABASES.items():
        db_list.append({
            "name": name,
            "path": str(path),
            "exists": path.exists()
        })
    return {"success": True, "databases": db_list}


@router.get("/api/v1/ui/databases/{db_name}/tables")
async def list_tables(db_name: str = FastAPIPath(..., description="数据库名称")):
    """获取指定数据库的所有表"""
    if db_name not in DATABASES:
        raise HTTPException(status_code=404, detail=f"数据库不存在：{db_name}")

    db_path = DATABASES[db_name]
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"数据库文件不存在：{db_name}")

    async with get_db_connection(db_path) as db:
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = await cursor.fetchall()

    return {
        "success": True,
        "database": db_name,
        "tables": [t[0] for t in tables if not t[0].startswith('sqlite_')]
    }


@router.get("/api/v1/ui/databases/{db_name}/tables/{table_name}/stats")
async def get_table_stats(
    db_name: str = FastAPIPath(..., description="数据库名称"),
    table_name: str = FastAPIPath(..., description="表名")
):
    """Get table statistics."""
    if db_name not in DATABASES:
        raise HTTPException(status_code=404, detail=f"数据库不存在：{db_name}")

    db_path = DATABASES[db_name]
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"数据库文件不存在：{db_name}")

    async with get_db_connection(db_path) as db:
        db.row_factory = aiosqlite.Row

        # 检查表是否存在
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        if not await cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"表不存在：{table_name}")

        # 总记录数
        cursor = await db.execute(f"SELECT COUNT(*) as total FROM {table_name}")
        result = await cursor.fetchone()
        total_records = result[0] if result else 0

        # 获取表的列信息
        cursor = await db.execute(f"PRAGMA table_info({table_name})")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]

        # 查找可能的日期列（扩展支持更多日期字段类型）
        date_columns = [
            'trade_date', 'report_date', 'date', 'created_at', 'updated_at',
            'week_start_date', 'week_end_date', 'detect_time', 'exec_time',
            'last_update_time', 'report_quarter'
        ]
        date_stats = {}

        for date_col in date_columns:
            if date_col in column_names:
                # 最早日期
                cursor = await db.execute(
                    f"SELECT MIN({date_col}) as earliest FROM {table_name}"
                )
                earliest = await cursor.fetchone()
                # 最新日期
                cursor = await db.execute(
                    f"SELECT MAX({date_col}) as latest FROM {table_name}"
                )
                latest = await cursor.fetchone()

                date_stats[date_col] = {
                    "earliest": earliest[0] if earliest else None,
                    "latest": latest[0] if latest else None
                }

        # 确定主日期字段（用于筛选）
        primary_date_field = None
        if 'trade_date' in column_names:
            primary_date_field = 'trade_date'
        elif 'report_date' in column_names:
            primary_date_field = 'report_date'
        elif 'week_start_date' in column_names:
            primary_date_field = 'week_start_date'
        elif 'date' in column_names:
            primary_date_field = 'date'

        # 查找主键列
        pk_columns = [col[1] for col in columns if col[5] > 0]

        # 确定表的类型（用于前端适配筛选逻辑）
        table_type = "standard"
        if table_name == 'weekly_kline_data':
            table_type = "weekly"
        elif table_name == 'stock_monthly_factors':
            table_type = "monthly"

        return {
            "success": True,
            "stats": {
                "total_records": total_records,
                "date_stats": date_stats,
                "primary_date_field": primary_date_field,
                "table_type": table_type,
                "primary_keys": pk_columns,
                "columns": column_names
            }
        }


@router.get("/api/v1/ui/databases/{db_name}/tables/{table_name}/columns")
async def get_table_columns(
    db_name: str = FastAPIPath(..., description="数据库名称"),
    table_name: str = FastAPIPath(..., description="表名")
):
    """获取表的列结构"""
    if db_name not in DATABASES:
        raise HTTPException(status_code=404, detail=f"数据库不存在：{db_name}")

    db_path = DATABASES[db_name]
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"数据库文件不存在：{db_name}")

    async with get_db_connection(db_path) as db:
        cursor = await db.execute(f"PRAGMA table_info({table_name})")
        columns = await cursor.fetchall()

        column_info = []
        for col in columns:
            column_info.append({
                "field": col[1],
                "type": col[2],
                "not_null": bool(col[3]),
                "default": col[4],
                "primary_key": bool(col[5])
            })

    return {
        "success": True,
        "database": db_name,
        "table": table_name,
        "columns": column_info
    }


@router.get("/api/v1/ui/databases/{db_name}/tables/{table_name}/data")
async def get_table_data(
    db_name: str = FastAPIPath(..., description="数据库名称"),
    table_name: str = FastAPIPath(..., description="表名"),
    # 筛选参数
    stock_code: Optional[str] = Query(None, description="股票代码（支持单个或多个，逗号分隔）"),
    stock_codes: Optional[str] = Query(None, description="股票代码列表（逗号分隔）"),
    trade_date: Optional[str] = Query(None, description="交易日期（单日）"),
    start_date: Optional[str] = Query(None, description="开始日期"),
    end_date: Optional[str] = Query(None, description="结束日期"),
    date_range: Optional[str] = Query(None, description="日期范围快捷方式：last_30d, last_90d, ytd"),
    industry: Optional[str] = Query(None, description="行业分类"),
    # 分页和排序
    page: int = Query(1, description="页码", ge=1),
    page_size: int = Query(100, description="每页数量", ge=10, le=10000),
    limit: Optional[int] = Query(None, description="返回条数（与 page_size 二选一）"),
    offset: Optional[int] = Query(0, description="偏移量"),
    order_by: Optional[str] = Query(None, description="排序字段"),
    order: Optional[str] = Query("desc", description="排序方向：asc 或 desc"),
    sort_config: Optional[str] = Query(None, description="组合排序配置（JSON格式）"),
    fields: Optional[str] = Query(None, description="返回字段（逗号分隔）")
):
    """Get table data with pagination and filtering."""
    if db_name not in DATABASES:
        raise HTTPException(status_code=404, detail=f"数据库不存在：{db_name}")

    db_path = DATABASES[db_name]
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"数据库文件不存在：{db_name}")

    async with get_db_connection(db_path) as db:
        db.row_factory = aiosqlite.Row

        # 检查表是否存在
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        if not await cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"表不存在：{table_name}")

        # 获取列信息
        cursor = await db.execute(f"PRAGMA table_info({table_name})")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]

        # 构建 WHERE 条件
        conditions = []
        params = []

        # 股票代码筛选（需要标准化格式）
        if stock_code:
            codes = [normalize_stock_code(c.strip()) for c in stock_code.split(',')]
            if len(codes) == 1:
                conditions.append("stock_code = ?")
                params.append(codes[0])
            else:
                placeholders = ','.join(['?' for _ in codes])
                conditions.append(f"stock_code IN ({placeholders})")
                params.extend(codes)
        elif stock_codes:
            codes = [normalize_stock_code(c.strip()) for c in stock_codes.split(',')]
            placeholders = ','.join(['?' for _ in codes])
            conditions.append(f"stock_code IN ({placeholders})")
            params.extend(codes)

        # 确定日期字段
        date_field = None
        if 'trade_date' in column_names:
            date_field = 'trade_date'
        elif 'report_date' in column_names:
            date_field = 'report_date'
        elif 'week_start_date' in column_names:
            # 对于周K线表，使用 week_start_date 作为主日期字段
            date_field = 'week_start_date'
        elif 'date' in column_names:
            date_field = 'date'

        # 日期筛选
        if date_field:
            if trade_date:
                conditions.append(f"{date_field} = ?")
                params.append(trade_date)

            if start_date:
                # 对于周K线，开始日期筛选：week_end_date >= start_date（周的结束日期在范围内）
                if table_name == 'weekly_kline_data' and 'week_end_date' in column_names:
                    conditions.append("week_end_date >= ?")
                    params.append(start_date)
                else:
                    conditions.append(f"{date_field} >= ?")
                    params.append(start_date)

            if end_date:
                # 对于周K线，结束日期筛选：week_start_date <= end_date（周的起始日期在范围内）
                if table_name == 'weekly_kline_data' and 'week_start_date' in column_names:
                    conditions.append("week_start_date <= ?")
                    params.append(end_date)
                else:
                    conditions.append(f"{date_field} <= ?")
                    params.append(end_date)

            # 日期范围快捷方式
            if date_range:
                from datetime import date
                today = date.today()
                range_start = None
                if date_range == 'last_30d':
                    range_start = today - timedelta(days=30)
                elif date_range == 'last_90d':
                    range_start = today - timedelta(days=90)
                elif date_range == 'ytd':
                    range_start = date(today.year, 1, 1)

                if range_start:
                    range_start_str = range_start.strftime('%Y-%m-%d')
                    if table_name == 'weekly_kline_data' and 'week_end_date' in column_names:
                        conditions.append("week_end_date >= ?")
                        params.append(range_start_str)
                    else:
                        conditions.append(f"{date_field} >= ?")
                        params.append(range_start_str)

        # 行业筛选
        if industry:
            # 检查是否有 industry 列
            if 'industry' in column_names:
                conditions.append("industry = ?")
                params.append(industry)
            elif 'sw_level1' in column_names:
                conditions.append("sw_level1 = ?")
                params.append(industry)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # 字段选择
        select_fields = "*"
        if fields:
            field_list = [f.strip() for f in fields.split(',')]
            # 确保 stock_code 和 trade_date 始终包含（用于排序和识别）
            if 'stock_code' not in field_list and 'stock_code' in column_names:
                field_list.insert(0, 'stock_code')
            valid_fields = [f for f in field_list if f in column_names]
            if valid_fields:
                select_fields = ','.join(valid_fields)

        # 安全检查：确保 order_by 字段存在于表中
        if order_by and order_by not in column_names:
            # 如果传入的排序字段不存在，忽略并使用默认排序
            order_by = None

        # 处理排序配置
        order_clause = ""
        if sort_config:
            # 组合排序：解析 JSON 格式的排序配置
            import json
            try:
                sort_list = json.loads(sort_config)
                order_parts = []
                for sort_item in sort_list:
                    field = sort_item.get("field")
                    order_dir = sort_item.get("order", "desc")
                    if field in column_names:
                        sql_dir = "DESC" if order_dir.lower() == "desc" else "ASC"
                        order_parts.append(f"{field} {sql_dir}")
                if order_parts:
                    order_clause = " ORDER BY " + ", ".join(order_parts)
            except json.JSONDecodeError:
                pass  # JSON 解析失败，使用默认排序

        if not order_clause:
            # 单字段排序或默认排序
            if not order_by:
                # 优先使用日期字段作为默认排序
                if date_field:
                    order_by = date_field
                else:
                    pk_columns = [col[1] for col in columns if col[5] > 0]
                    order_by = pk_columns[0] if pk_columns else column_names[0] if column_names else "rowid"

            order_dir = "DESC" if order and order.lower() == "desc" else "ASC"
            order_clause = f" ORDER BY {order_by} {order_dir}"

        # 分页
        if limit is not None:
            actual_page_size = min(limit, 10000)
            actual_offset = offset if offset else 0
        else:
            actual_page_size = page_size
            actual_offset = (page - 1) * page_size

        # 查询数据
        query = f"SELECT {select_fields} FROM {table_name} WHERE {where_clause}{order_clause} LIMIT ? OFFSET ?"
        params.extend([actual_page_size, actual_offset])

        cursor = await db.execute(query, tuple(params))
        rows = await cursor.fetchall()
        data = [dict(row) for row in rows]

        # 查询总数
        cursor = await db.execute(f"SELECT COUNT(*) as total FROM {table_name} WHERE {where_clause}", tuple(params[:-2]))
        result = await cursor.fetchone()
        total = result[0] if result else 0

    return {
        "success": True,
        "database": db_name,
        "table": table_name,
        "columns": list(data[0].keys()) if data else column_names,
        "data": data,
        "pagination": {
            "page": page if limit is None else (actual_offset // actual_page_size) + 1,
            "page_size": actual_page_size,
            "total": total,
            "total_pages": (total + actual_page_size - 1) // actual_page_size,
            "offset": actual_offset,
            "limit": actual_page_size
        },
        "query_info": {
            "execution_time_ms": 0,
            "filters_applied": conditions
        }
    }


@router.put("/api/v1/ui/databases/{db_name}/tables/{table_name}/update")
async def update_table_record(
    db_name: str = FastAPIPath(..., description="数据库名称"),
    table_name: str = FastAPIPath(..., description="表名"),
    where_clause: str = Body(..., description="WHERE 子句（不含 WHERE 关键字）"),
    data: Dict[str, Any] = Body(..., description="要更新的数据"),
    where_params: List[Any] = Body([], description="WHERE 子句参数")
):
    """更新表数据"""
    if db_name not in DATABASES:
        raise HTTPException(status_code=404, detail=f"数据库不存在：{db_name}")

    db_path = DATABASES[db_name]
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"数据库文件不存在：{db_name}")

    # 安全检查：不允许更新系统表
    if table_name.startswith('sqlite_'):
        raise HTTPException(status_code=400, detail="不允许修改系统表")

    async with get_db_connection(db_path) as db:
        # 构建 SET 子句
        set_clause = ", ".join([f"{k} = ?" for k in data.keys()])

        # 执行更新
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        params = tuple(data.values()) + tuple(where_params)

        cursor = await db.execute(query, params)
        await db.commit()

        return {
            "success": True,
            "message": f"已更新 {cursor.rowcount} 条记录",
            "rows_affected": cursor.rowcount
        }


@router.delete("/api/v1/ui/databases/{db_name}/tables/{table_name}/delete")
async def delete_table_record(
    db_name: str = FastAPIPath(..., description="数据库名称"),
    table_name: str = FastAPIPath(..., description="表名"),
    where_clause: str = Body(..., description="WHERE 子句（不含 WHERE 关键字）"),
    where_params: List[Any] = Body([], description="WHERE 子句参数")
):
    """删除表数据"""
    if db_name not in DATABASES:
        raise HTTPException(status_code=404, detail=f"数据库不存在：{db_name}")

    db_path = DATABASES[db_name]
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"数据库文件不存在：{db_name}")

    # 安全检查：不允许删除系统表
    if table_name.startswith('sqlite_'):
        raise HTTPException(status_code=400, detail="不允许修改系统表")

    async with get_db_connection(db_path) as db:
        # 执行删除
        query = f"DELETE FROM {table_name} WHERE {where_clause}"
        cursor = await db.execute(query, tuple(where_params))
        await db.commit()

        return {
            "success": True,
            "message": f"已删除 {cursor.rowcount} 条记录",
            "rows_affected": cursor.rowcount
        }


@router.post("/api/v1/ui/databases/{db_name}/tables/{table_name}/query")
async def query_table_data(
    db_name: str = FastAPIPath(..., description="数据库名称"),
    table_name: str = FastAPIPath(..., description="表名"),
    request: QueryRequest = Body(..., description="查询请求")
):
    """
    高级筛选查询接口

    支持的筛选操作符：
    - eq: 等于 (=)
    - ne: 不等于 (!=)
    - gt: 大于 (>)
    - gte: 大于等于 (>=)
    - lt: 小于 (<)
    - lte: 小于等于 (<=)
    - in: 属于 (IN)
    - not_in: 不属于 (NOT IN)
    - between: 在...之间 (BETWEEN)
    - like: 模糊匹配 (LIKE)
    - is_null: 为空 (IS NULL)
    - is_not_null: 不为空 (IS NOT NULL)

    示例请求体:
    ```json
    {
        "filters": [
            {"field": "stock_code", "operator": "in", "value": ["000001.SZ", "600519.SH"]},
            {"field": "circ_market_cap", "operator": "lt", "value": 50000000000},
            {"field": "pe_inverse", "operator": "gt", "value": 0.02},
            {"field": "industry", "operator": "eq", "value": "银行"}
        ],
        "fields": ["stock_code", "stock_name", "circ_market_cap", "pe_inverse"],
        "sort": [
            {"field": "circ_market_cap", "order": "asc"}
        ],
        "limit": 100,
        "offset": 0
    }
    ```
    """
    if db_name not in DATABASES:
        raise HTTPException(status_code=404, detail=f"数据库不存在：{db_name}")

    db_path = DATABASES[db_name]
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"数据库文件不存在：{db_name}")

    async with get_db_connection(db_path) as db:
        db.row_factory = aiosqlite.Row

        # 检查表是否存在
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        if not await cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"表不存在：{table_name}")

        # 获取列信息
        cursor = await db.execute(f"PRAGMA table_info({table_name})")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]

        # 构建 WHERE 条件
        conditions = []
        params = []

        for filter_cond in request.filters:
            field = filter_cond.field
            op = filter_cond.operator.lower()
            value = filter_cond.value
            value2 = filter_cond.value2

            # 字段存在性检查
            if field not in column_names:
                raise HTTPException(status_code=400, detail=f"字段不存在：{field}")

            # 特殊处理 stock_code 字段的格式标准化
            if field == 'stock_code' and op in ('eq', 'in', 'not_in'):
                if op == 'eq':
                    value = normalize_stock_code(value)
                elif op in ('in', 'not_in'):
                    if isinstance(value, list):
                        value = [normalize_stock_code(code) for code in value]

            # 根据操作符构建条件
            if op == 'eq':
                conditions.append(f"{field} = ?")
                params.append(value)
            elif op == 'ne':
                conditions.append(f"{field} != ?")
                params.append(value)
            elif op == 'gt':
                conditions.append(f"{field} > ?")
                params.append(value)
            elif op == 'gte':
                conditions.append(f"{field} >= ?")
                params.append(value)
            elif op == 'lt':
                conditions.append(f"{field} < ?")
                params.append(value)
            elif op == 'lte':
                conditions.append(f"{field} <= ?")
                params.append(value)
            elif op == 'in':
                if not isinstance(value, list):
                    raise HTTPException(status_code=400, detail=f"in 操作符的值必须是列表：{field}")
                placeholders = ','.join(['?' for _ in value])
                conditions.append(f"{field} IN ({placeholders})")
                params.extend(value)
            elif op == 'not_in':
                if not isinstance(value, list):
                    raise HTTPException(status_code=400, detail=f"not_in 操作符的值必须是列表：{field}")
                placeholders = ','.join(['?' for _ in value])
                conditions.append(f"{field} NOT IN ({placeholders})")
                params.extend(value)
            elif op == 'between':
                conditions.append(f"{field} BETWEEN ? AND ?")
                params.append(value)
                params.append(value2)
            elif op == 'like':
                conditions.append(f"{field} LIKE ?")
                params.append(f"%{value}%")
            elif op == 'is_null':
                conditions.append(f"{field} IS NULL")
            elif op == 'is_not_null':
                conditions.append(f"{field} IS NOT NULL")
            else:
                raise HTTPException(status_code=400, detail=f"不支持的操作符：{op}")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # 字段选择
        if request.fields:
            valid_fields = [f for f in request.fields if f in column_names]
            if valid_fields:
                select_fields = ','.join(valid_fields)
            else:
                select_fields = "*"
        else:
            select_fields = "*"

        # 排序
        order_clause = ""
        if request.sort:
            # 用户指定排序
            order_parts = []
            for sort_opt in request.sort:
                if sort_opt.field in column_names:
                    order_dir = "DESC" if sort_opt.order.lower() == "desc" else "ASC"
                    order_parts.append(f"{sort_opt.field} {order_dir}")
            if order_parts:
                order_clause = " ORDER BY " + ", ".join(order_parts)
        else:
            # 智能排序策略：根据筛选条件自动决定排序方式
            order_parts = []

            # 检查筛选了哪些字段
            filter_fields = [f.field for f in request.filters]

            # 1. 篮选了股票代码 → 按股票代码分组，日期降序
            if 'stock_code' in filter_fields and 'stock_code' in column_names:
                order_parts.append("stock_code ASC")
                if 'trade_date' in column_names:
                    order_parts.append("trade_date DESC")

            # 2. 篮选了行业 → 按行业分组，日期降序
            elif ('sw_level1' in filter_fields or 'industry' in filter_fields):
                if 'sw_level1' in column_names:
                    order_parts.append("sw_level1 ASC")
                if 'trade_date' in column_names:
                    order_parts.append("trade_date DESC")

            # 3. 篛选了市值范围 → 按市值降序
            elif 'circ_market_cap' in filter_fields or 'total_market_cap' in filter_fields:
                if 'circ_market_cap' in column_names:
                    order_parts.append("circ_market_cap DESC")
                elif 'total_market_cap' in column_names:
                    order_parts.append("total_market_cap DESC")

            # 4. 默认：按日期降序
            elif 'trade_date' in column_names:
                order_parts.append("trade_date DESC")
            elif 'date' in column_names:
                order_parts.append("date DESC")
            else:
                # 无日期列，按主键排序
                pk_columns = [col[1] for col in columns if col[5] > 0]
                if pk_columns:
                    order_parts.append(f"{pk_columns[0]} DESC")

            if order_parts:
                order_clause = " ORDER BY " + ", ".join(order_parts)

        # 分页
        query = f"SELECT {select_fields} FROM {table_name} WHERE {where_clause}{order_clause} LIMIT ? OFFSET ?"
        params.extend([request.limit, request.offset])

        cursor = await db.execute(query, tuple(params))
        rows = await cursor.fetchall()
        data = [dict(row) for row in rows]

        # 查询总数
        count_params = tuple(params[:-2]) if len(params) >= 2 else ()
        cursor = await db.execute(f"SELECT COUNT(*) as total FROM {table_name} WHERE {where_clause}", count_params)
        result = await cursor.fetchone()
        total = result[0] if result else 0

    return {
        "success": True,
        "database": db_name,
        "table": table_name,
        "columns": list(data[0].keys()) if data else column_names,
        "data": data,
        "pagination": {
            "limit": request.limit,
            "offset": request.offset,
            "total": total,
            "has_more": (request.offset + request.limit) < total
        },
        "query_info": {
            "filters_count": len(request.filters),
            "filters_applied": conditions
        }
    }


@router.post("/api/v1/ui/databases/{db_name}/tables/{table_name}/aggregate")
async def aggregate_table_data(
    db_name: str = FastAPIPath(..., description="数据库名称"),
    table_name: str = FastAPIPath(..., description="表名"),
    request: AggregateRequest = Body(..., description="聚合请求")
):
    """
    聚合统计接口

    支持的聚合操作：
    - count: 计数
    - sum: 求和
    - avg: 平均
    - max: 最大值
    - min: 最小值

    示例请求体:
    ```json
    {
        "group_by": ["sw_level1"],
        "aggregations": [
            {"field": "stock_code", "agg": "count", "alias": "stock_count"},
            {"field": "circ_market_cap", "agg": "avg", "alias": "avg_market_cap"}
        ],
        "filters": [
            {"field": "trade_date", "operator": "eq", "value": "2026-04-03"}
        ]
    }
    ```
    """
    if db_name not in DATABASES:
        raise HTTPException(status_code=404, detail=f"数据库不存在：{db_name}")

    db_path = DATABASES[db_name]
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"数据库文件不存在：{db_name}")

    async with get_db_connection(db_path) as db:
        db.row_factory = aiosqlite.Row

        # 检查表是否存在
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        if not await cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"表不存在：{table_name}")

        # 获取列信息
        cursor = await db.execute(f"PRAGMA table_info({table_name})")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]

        # 构建 WHERE 条件
        conditions = []
        params = []

        if request.filters:
            for filter_cond in request.filters:
                field = filter_cond.field
                op = filter_cond.operator.lower()
                value = filter_cond.value
                value2 = filter_cond.value2

                if field not in column_names:
                    raise HTTPException(status_code=400, detail=f"字段不存在：{field}")

                if op == 'eq':
                    conditions.append(f"{field} = ?")
                    params.append(value)
                elif op == 'ne':
                    conditions.append(f"{field} != ?")
                    params.append(value)
                elif op == 'gt':
                    conditions.append(f"{field} > ?")
                    params.append(value)
                elif op == 'gte':
                    conditions.append(f"{field} >= ?")
                    params.append(value)
                elif op == 'lt':
                    conditions.append(f"{field} < ?")
                    params.append(value)
                elif op == 'lte':
                    conditions.append(f"{field} <= ?")
                    params.append(value)
                elif op == 'in':
                    if not isinstance(value, list):
                        raise HTTPException(status_code=400, detail=f"in 操作符的值必须是列表")
                    placeholders = ','.join(['?' for _ in value])
                    conditions.append(f"{field} IN ({placeholders})")
                    params.extend(value)
                elif op == 'not_in':
                    if not isinstance(value, list):
                        raise HTTPException(status_code=400, detail=f"not_in 操作符的值必须是列表")
                    placeholders = ','.join(['?' for _ in value])
                    conditions.append(f"{field} NOT IN ({placeholders})")
                    params.extend(value)
                elif op == 'between':
                    conditions.append(f"{field} BETWEEN ? AND ?")
                    params.append(value)
                    params.append(value2)
                elif op == 'like':
                    conditions.append(f"{field} LIKE ?")
                    params.append(f"%{value}%")
                elif op == 'is_null':
                    conditions.append(f"{field} IS NULL")
                elif op == 'is_not_null':
                    conditions.append(f"{field} IS NOT NULL")
                else:
                    raise HTTPException(status_code=400, detail=f"不支持的操作符：{op}")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # 构建聚合字段
        agg_mapping = {
            'count': 'COUNT',
            'sum': 'SUM',
            'avg': 'AVG',
            'max': 'MAX',
            'min': 'MIN'
        }

        agg_parts = []
        for agg in request.aggregations:
            field = agg.get('field', '*')
            agg_func = agg.get('agg', 'count').lower()
            alias = agg.get('alias', f"{agg_func}_{field}")

            if agg_func not in agg_mapping:
                raise HTTPException(status_code=400, detail=f"不支持的聚合操作：{agg_func}")

            sql_func = agg_mapping[agg_func]
            if field == '*':
                agg_parts.append(f"{sql_func}(*) as {alias}")
            else:
                if field not in column_names:
                    raise HTTPException(status_code=400, detail=f"字段不存在：{field}")
                agg_parts.append(f"{sql_func}({field}) as {alias}")

        # 构建 GROUP BY
        group_by_clause = ""
        if request.group_by:
            valid_group_fields = [f for f in request.group_by if f in column_names]
            if valid_group_fields:
                group_by_clause = " GROUP BY " + ", ".join(valid_group_fields)

        # 执行查询
        select_parts = []
        if request.group_by:
            select_parts.extend(request.group_by)
        select_parts.extend(agg_parts)

        query = f"SELECT {', '.join(select_parts)} FROM {table_name} WHERE {where_clause}{group_by_clause}"

        cursor = await db.execute(query, tuple(params))
        rows = await cursor.fetchall()
        data = [dict(row) for row in rows]

    return {
        "success": True,
        "database": db_name,
        "table": table_name,
        "group_by": request.group_by or [],
        "aggregations": data,
        "query_info": {
            "filters_count": len(request.filters) if request.filters else 0,
            "rows_returned": len(data)
        }
    }
