CREATE DATABASE IF NOT EXISTS agent_runtime CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE agent_runtime;

CREATE TABLE IF NOT EXISTS agent_run (
  run_id VARCHAR(64) PRIMARY KEY,
  tenant_id VARCHAR(128) NOT NULL,
  agent_id VARCHAR(128) NOT NULL,
  status VARCHAR(32) NOT NULL,
  version BIGINT NOT NULL DEFAULT 0,
  input LONGTEXT NOT NULL,
  output LONGTEXT,
  current_node_id VARCHAR(64),
  max_steps INT NOT NULL DEFAULT 50,
  steps INT NOT NULL DEFAULT 0,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS agent_node (
  node_id VARCHAR(64) PRIMARY KEY,
  run_id VARCHAR(64) NOT NULL,
  parent_node_id VARCHAR(64),
  type VARCHAR(32) NOT NULL,
  name VARCHAR(128) NOT NULL,
  input LONGTEXT,
  output LONGTEXT,
  status VARCHAR(32) NOT NULL,
  attempt INT NOT NULL DEFAULT 0,
  version BIGINT NOT NULL DEFAULT 0,
  lease_owner VARCHAR(128),
  lease_until DATETIME(6),
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  started_at DATETIME(6),
  finished_at DATETIME(6),
  INDEX idx_node_run (run_id),
  INDEX idx_node_recovery (status, lease_until),
  CONSTRAINT fk_node_run FOREIGN KEY (run_id) REFERENCES agent_run(run_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS agent_edge (
  run_id VARCHAR(64) NOT NULL,
  from_node_id VARCHAR(64) NOT NULL,
  to_node_id VARCHAR(64) NOT NULL,
  PRIMARY KEY (run_id, from_node_id, to_node_id),
  CONSTRAINT fk_edge_run FOREIGN KEY (run_id) REFERENCES agent_run(run_id) ON DELETE CASCADE,
  CONSTRAINT fk_edge_from FOREIGN KEY (from_node_id) REFERENCES agent_node(node_id) ON DELETE CASCADE,
  CONSTRAINT fk_edge_to FOREIGN KEY (to_node_id) REFERENCES agent_node(node_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS checkpoint (
  run_id VARCHAR(64) PRIMARY KEY,
  graph_version BIGINT NOT NULL,
  current_node_id VARCHAR(64),
  state_json JSON NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  CONSTRAINT fk_checkpoint_run FOREIGN KEY (run_id) REFERENCES agent_run(run_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS tool_call (
  call_id VARCHAR(64) PRIMARY KEY,
  run_id VARCHAR(64) NOT NULL,
  node_id VARCHAR(64) NOT NULL,
  tool_name VARCHAR(128) NOT NULL,
  idempotency_key VARCHAR(255) NOT NULL UNIQUE,
  status VARCHAR(32) NOT NULL,
  input LONGTEXT,
  output LONGTEXT,
  attempt INT NOT NULL DEFAULT 0,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  CONSTRAINT fk_tool_run FOREIGN KEY (run_id) REFERENCES agent_run(run_id) ON DELETE CASCADE,
  CONSTRAINT fk_tool_node FOREIGN KEY (node_id) REFERENCES agent_node(node_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS event_outbox (
  event_id VARCHAR(64) PRIMARY KEY,
  event_type VARCHAR(128) NOT NULL,
  aggregate_id VARCHAR(64) NOT NULL,
  payload LONGTEXT NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
  attempts INT NOT NULL DEFAULT 0,
  next_attempt_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  published_at DATETIME(6),
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  INDEX idx_outbox_poll (status, next_attempt_at, created_at)
) ENGINE=InnoDB;
