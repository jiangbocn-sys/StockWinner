"""
Kronos 模型服务

在应用启动时加载 Kronos 预测器（单例），为策略沙盒提供封装好的预测函数。
策略代码无需 import os/sys/safetensors，直接调用沙盒注入的 kronos_predict()。
"""

import os
import sys
from typing import Optional, Dict, Any
import pandas as pd


class KronosService:
    """Kronos 模型单例服务"""

    # Kronos 模型默认配置
    TOKENIZER_CONFIG = {
        'd_in': 6, 'd_model': 256, 'n_heads': 4, 'ff_dim': 512,
        'n_enc_layers': 4, 'n_dec_layers': 4, 'ffn_dropout_p': 0.0,
        'attn_dropout_p': 0.0, 'resid_dropout_p': 0.0,
        's1_bits': 10, 's2_bits': 10, 'beta': 0.05,
        'gamma0': 1.0, 'gamma': 1.1, 'zeta': 0.05, 'group_size': 4
    }
    MODEL_CONFIG = {
        's1_bits': 10, 's2_bits': 10, 'n_layers': 6,
        'd_model': 512, 'n_heads': 8, 'ff_dim': 1024,
        'ffn_dropout_p': 0.0, 'attn_dropout_p': 0.0,
        'resid_dropout_p': 0.0, 'token_dropout_p': 0.0, 'learn_te': False
    }

    def __init__(self):
        self._predictor = None
        self._error = None
        self._loaded = False

    def load(self, use_base: bool = False) -> bool:
        """
        加载 Kronos 模型（全局仅加载一次）

        Args:
            use_base: 是否使用 base 模型（默认 small）

        Returns:
            是否加载成功
        """
        if self._loaded:
            return self._predictor is not None

        self._loaded = True

        try:
            # 设置 HuggingFace 国内镜像
            os.environ.setdefault('HF_ENDPOINT', 'https://hf-mirror.com')

            # 定位 Kronos 目录
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            kronos_dir = os.path.join(project_root, "deps", "Kronos")
            weights_dir = os.path.join(kronos_dir, "weights")

            if not os.path.exists(kronos_dir):
                self._error = f"Kronos 目录不存在: {kronos_dir}"
                return False

            # 将 Kronos 加入模块搜索路径
            if kronos_dir not in sys.path:
                sys.path.insert(0, kronos_dir)

            from model import Kronos, KronosTokenizer, KronosPredictor
            import safetensors
            import torch

            # 查找权重文件
            def find_weight(model_name: str) -> Optional[str]:
                weight_path = os.path.join(weights_dir, model_name)
                if os.path.exists(weight_path):
                    for f in os.listdir(weight_path):
                        if f.endswith('.safetensors'):
                            return os.path.join(weight_path, f)
                return None

            tok_path = find_weight('Kronos-Tokenizer-base')
            model_name = 'Kronos-base' if use_base else 'Kronos-small'
            model_path = find_weight(model_name)

            tokenizer = KronosTokenizer(**self.TOKENIZER_CONFIG)
            model = Kronos(**self.MODEL_CONFIG)

            if tok_path:
                tokenizer.load_state_dict(safetensors.torch.load_file(tok_path), strict=False)
            if model_path:
                model.load_state_dict(safetensors.torch.load_file(model_path), strict=False)

            device = 'cuda:0' if torch.cuda.is_available() else 'cpu'
            model = model.to(device)
            tokenizer = tokenizer.to(device)

            self._predictor = KronosPredictor(model, tokenizer, max_context=512)
            self._device = device
            print(f"[Kronos] 模型加载成功: {model_name} on {device}")
            return True

        except Exception as e:
            self._error = str(e)
            print(f"[Kronos] 模型加载失败: {e}")
            return False

    def predict(
        self,
        df_hist: pd.DataFrame,
        pred_len: int = 5,
        future_dates: Optional[pd.DatetimeIndex] = None,
        temperature: float = 1.0,
        top_p: float = 0.9,
        sample_count: int = 10,
    ) -> Optional[pd.DataFrame]:
        """
        使用 Kronos 模型进行时间序列预测

        Args:
            df_hist: 历史数据 DataFrame，需包含 open/high/low/close/volume/amount
            pred_len: 预测长度（默认 5 天）
            future_dates: 未来日期索引（可选，不提供则自动生成工作日）
            temperature: 采样温度
            top_p: Top-p 采样阈值
            sample_count: 采样次数

        Returns:
            预测结果 DataFrame（包含 open/high/low/close/volume/amount），失败返回 None
        """
        if self._predictor is None:
            return None

        try:
            lookback = len(df_hist) - pred_len
            if lookback < 20:
                return None

            df_input = df_hist.loc[:lookback - 1, ['open', 'high', 'low', 'close', 'volume', 'amount']].reset_index(drop=True)

            # 生成未来日期
            if future_dates is None:
                last_ts = df_hist.iloc[-1].get('timestamps') or df_hist.iloc[-1].get('trade_date')
                if isinstance(last_ts, str):
                    last_ts = pd.Timestamp(last_ts)
                future_dates = pd.bdate_range(start=pd.Timestamp(last_ts) + pd.Timedelta(days=1), periods=pred_len)

            x_ts = pd.Series(df_hist.loc[:lookback - 1, 'timestamps'].values, dtype='datetime64[ns]') if 'timestamps' in df_hist.columns else pd.Series(pd.date_range(end=pd.Timestamp(df_hist.iloc[-1].get('trade_date', '2026-01-01')), periods=lookback))
            y_ts = pd.Series(future_dates, dtype='datetime64[ns]')

            pred_df = self._predictor.predict(
                df=df_input,
                x_timestamp=x_ts.reset_index(drop=True),
                y_timestamp=y_ts.reset_index(drop=True),
                pred_len=pred_len,
                T=temperature,
                top_p=top_p,
                sample_count=sample_count,
            )
            return pred_df

        except Exception as e:
            print(f"[Kronos] 预测失败: {e}")
            return None

    @property
    def is_available(self) -> bool:
        return self._predictor is not None

    @property
    def error(self) -> Optional[str]:
        return self._error

    @property
    def device(self) -> str:
        return getattr(self, '_device', 'unknown')


# ── 全局单例 ───────────────────────────────────────────
_service: Optional[KronosService] = None


def get_kronos_service() -> KronosService:
    """获取 Kronos 服务单例"""
    global _service
    if _service is None:
        _service = KronosService()
    return _service


def load_kronos_on_startup(use_base: bool = False):
    """在应用启动时预加载 Kronos 模型（非阻塞）"""
    svc = get_kronos_service()
    svc.load(use_base=use_base)
