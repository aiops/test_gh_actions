import logging

from logsight.analytics_core.logs import LogBatch
from logsight.analytics_core.modules.risk_analysis.risk_analysis import RiskAnalysis
from .core import TransformModule

logger = logging.getLogger("logsight." + __name__)


class RiskScoreModule(TransformModule):

    def __init__(self):
        super().__init__()
        self.risk_analysis = RiskAnalysis()

    def transform(self, data: LogBatch) -> LogBatch:
        data.logs = [self.risk_analysis.calculate_risk(log) for log in data.logs]
        return data
