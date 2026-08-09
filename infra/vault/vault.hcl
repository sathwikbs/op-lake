# HashiCorp Vault -- the platform SECRET STORE (Phase 2 hardening).
#
# Local reference posture: file storage backend persisted on the `vault_file`
# volume (survives restarts), single-node, TLS terminated at the Phase-3 gateway
# (in-cluster listener is plaintext on the admin network). Init/unseal + secret
# loading + rendering is done by the one-shot `vault-init` sidecar; only that
# sidecar and Dagster (runtime VaultSecretProvider) ever talk to Vault.
#
# Cloud: swap `storage "file"` for a HA backend (raft/consul) and enable
# auto-unseal via a cloud KMS (awskms/azurekeyvault/gcpckms) instead of the
# file-persisted unseal key used here for local dev.

storage "file" {
  path = "/vault/file"
}

listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = 1
}

disable_mlock = true
ui            = false
api_addr      = "http://vault:8200"
