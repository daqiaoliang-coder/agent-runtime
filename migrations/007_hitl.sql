USE agent_runtime;

CREATE TABLE IF NOT EXISTS run_interrupt (
  interrupt_id VARCHAR(64) PRIMARY KEY,
  run_id VARCHAR(64) NOT NULL,
  tenant_id VARCHAR(128) NOT NULL,
  node_id VARCHAR(64),
  reason VARCHAR(1024) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'WAITING',
  decision VARCHAR(255),
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  resolved_at DATETIME(6),
  UNIQUE KEY uk_interrupt_run (run_id, status),
  INDEX idx_interrupt_tenant (tenant_id, status),
  CONSTRAINT fk_interrupt_run FOREIGN KEY (run_id) REFERENCES agent_run(run_id) ON DELETE CASCADE
) ENGINE=InnoDB;
