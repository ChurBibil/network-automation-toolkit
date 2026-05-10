import asyncio
import logging
import json
import os
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("NetworkEngine")

class AuditResult(BaseModel):
    node: str
    is_up: bool
    latency_ms: Optional[float]
    timestamp: datetime = Field(default_factory=datetime.now)

class InfrastructureAuditor:
    def __init__(self, nodes: List[str], max_concurrency: int = 25):
        self.nodes = nodes
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.results = []

    async def _check_node(self, host: str) -> AuditResult:
        async with self.semaphore:
            start_time = datetime.now()
            # Профессиональный вызов subprocess через asyncio
            proc = await asyncio.create_subprocess_exec(
                'ping', '-c', '1', '-W', '2', host,
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.PIPE
            )
            try:
                await asyncio.wait_for(proc.wait(), timeout=3.5)
                is_up = proc.returncode == 0
                latency = (datetime.now() - start_time).total_seconds() * 1000 if is_up else None
                return AuditResult(node=host, is_up=is_up, latency_ms=latency)
            except asyncio.TimeoutError:
                try: proc.kill()
                except: pass
                return AuditResult(node=host, is_up=False, latency_ms=None)

    async def run_audit(self):
        logger.info(f"Initiating audit for {len(self.nodes)} endpoints...")
        tasks = [self._check_node(n) for n in self.nodes]
        self.results = await asyncio.gather(*tasks)
        self._export_results()

    def _export_results(self):
        os.makedirs("telemetry", exist_ok=True)
        filename = f"telemetry/report_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        with open(filename, 'w') as f:
            json.dump([r.model_dump(mode='json') for r in self.results], f, indent=4)
        logger.info(f"Telemetry report generated: {filename}")

if __name__ == "__main__":
    inventory = ["8.8.8.8", "1.1.1.1", "google.com", "yandex.ru", "127.0.0.1"]
    auditor = InfrastructureAuditor(inventory)
    asyncio.run(auditor.run_audit())