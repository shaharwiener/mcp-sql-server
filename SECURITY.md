# Security Documentation - MCP SQL Server

**Version**: 2.0  
**Date**: October 2025  
**Status**: Ready for Security Review

## Executive Summary

This document provides a comprehensive security overview of the MCP SQL Server implementation for enterprise security review (Cisco). The system is designed for internal VPN deployment with network-level access control and comprehensive audit logging.

## Deployment Context

- **Network**: Internal VPN (trusted network)
- **Authentication**: Network-level (VPN + database permissions) - No application-level authentication
- **Access Control**: VPN isolation + database-level permissions
- **Audit**: Comprehensive query and access logging
- **User Tracking**: All operations logged as "system" user

## Security Architecture

### 1. Input Validation & SQL Injection Protection

#### Implementation
- **Query Length Validation**: Maximum 10,000 characters per query
- **Table Name Validation**: Regex-based validation allowing only `[a-zA-Z0-9_]`
- **Database Whitelist**: Only MCPay, mobydom5, and Billing databases allowed
- **SQL Bracket Notation**: All table names escaped using SQL Server `[table_name]` syntax

#### Protection Against
- SQL injection via table names (e.g., `table:users'; DROP TABLE--`)
- Command stacking (multiple statements)
- Encoding attacks (null bytes, unicode escaping)
- Extended stored procedures (xp_cmdshell, OPENROWSET)

#### Code References
- `services/validation_service.py` - All validation logic
- `services/schema_service.py` - Lines 85-101 (table name validation)
- `config/security_config.py` - Security limits and patterns

### 2. Query Safety Controls

#### SELECT-Only Enforcement
- Regex pattern matching for SELECT statements
- Blocks: UPDATE, INSERT, DELETE, DROP, CREATE, ALTER, TRUNCATE, EXEC

#### Dangerous Pattern Detection
Blocks patterns including:
- `xp_cmdshell` - Command execution
- `OPENROWSET` / `OPENQUERY` - External data sources
- `sp_executesql` - Dynamic SQL execution
- Block comments `/* */` - Potential obfuscation

#### Query Complexity Limits
- Maximum query length: 10,000 characters
- Maximum result rows: 10,000 (auto-truncated with warning)
- Connection timeout: 30 seconds
- Command timeout: 300 seconds (max 600 seconds)

#### Code References
- `services/simple_query_service.py` - Lines 75-164 (validation pipeline)
- `services/validation_service.py` - Lines 68-102 (pattern detection)

### 3. Audit Logging

#### What is Logged
All operations are logged including:
- Query execution (SELECT statements)
- Schema access (table listings, structure queries)
- Failures and errors
- All queries attributed to "system" user

#### Log Format
JSON structured logs with:
- Timestamp (ISO format)
- User ID (always "system" for VPN deployment)
- Database accessed
- Query (sanitized - PII removed)
- Success/failure status
- Row count
- Execution time (milliseconds)
- Error message (if failed)

#### PII Sanitization in Logs
Automatically masks:
- String literals in queries (`'value'` → `'***'`)
- Phone numbers (`+972507328808` → `***PHONE***`)
- Email addresses (`user@example.com` → `***EMAIL***`)
- Connection strings (passwords masked)

#### Log Storage
- Location: Configurable (default: `./logs/audit/`)
- Rotation: Daily (filename: `audit_YYYYMMDD.log`)
- Format: JSON (one entry per line)
- Retention: Application-managed (implement rotation policy as needed)

#### Code References
- `services/audit_service.py` - Complete audit logging implementation
- `services/validation_service.py` - Lines 119-141 (PII sanitization)

### 4. Access Control

#### Network-Level Security
**Primary Security Model:**
- VPN network isolation (trusted internal network)
- Database-level permissions enforced by SQL Server
- No application-level authentication

#### User Tracking
For audit purposes:
- All operations attributed to "system" user
- Individual user tracking relies on VPN logs + database audit
- Simplified for internal trusted network deployment

#### Why No Application Auth?
- Internal VPN provides network-level access control
- Database permissions control what data can be accessed
- Reduces complexity and potential security misconfiguration
- Appropriate for internal QA/Dev tool use case

#### Code References
- `services/auth_service.py` - Minimal user tracking (returns "system")
- `server.py` - User ID assignment for audit logging

### 5. Error Handling & Information Disclosure

#### Generic Error Messages
External responses contain:
- Generic error messages ("Query execution failed")
- No stack traces
- No SQL syntax details
- No connection string information

#### Detailed Logging
Server-side logs contain:
- Full error details
- Stack traces
- Query details
- Connection information (with masked credentials)

#### Connection String Protection
- Passwords masked in all error messages
- User IDs masked in error responses
- Connection details never returned to client

#### Code References
- `services/simple_query_service.py` - Lines 221-242 (error handling)
- `services/schema_service.py` - Lines 130-145 (error handling)
- `services/validation_service.py` - Lines 143-155 (connection string masking)

### 6. Connection Security

#### Credential Management
- Credentials stored in AWS Systems Manager (SSM)
- Retrieved at runtime with decryption
- Never logged or exposed in error messages
- Automatic connection cleanup

#### Connection Pooling
- Automatic timeout enforcement
- Connection cleanup on errors
- Retry logic for transient failures

#### Code References
- `services/configuration_service.py` - SSM integration
- `services/db_connection_service.py` - Connection management

## Security Configurations

### Default Security Settings

```python
# config/security_config.py
MAX_QUERY_LENGTH = 10000          # Maximum query characters
MAX_RESULT_ROWS = 10000            # Maximum result rows
DEFAULT_CONNECTION_TIMEOUT = 30    # Seconds
DEFAULT_COMMAND_TIMEOUT = 300      # Seconds (5 minutes)
MAX_COMMAND_TIMEOUT = 600          # Seconds (10 minutes)
```

### Environment Variables

```bash
# Basic
PANGO_ENV=Int                      # Int, Stg, or Prd

# Audit Logging
AUDIT_LOG_ENABLED=true             # Enable audit logging
AUDIT_LOG_PATH=./logs/audit/       # Audit log directory
```

## Threat Model & Mitigations

| Threat | Mitigation | Status |
|--------|------------|--------|
| SQL Injection | Input validation, parameterized queries, table name escaping | ✅ Implemented |
| Command Injection | Pattern detection, SELECT-only enforcement | ✅ Implemented |
| Information Disclosure | Generic errors, connection string masking | ✅ Implemented |
| Unauthorized Access | VPN network isolation + database permissions | ✅ Implemented |
| Data Exfiltration | Result row limits, audit logging | ✅ Implemented |
| DoS - Query Complexity | Query length limits, timeout enforcement | ✅ Implemented |
| DoS - Result Size | Row count limits (10K), automatic truncation | ✅ Implemented |
| Credential Exposure | SSM storage, masked logging | ✅ Implemented |
| Audit Trail Tampering | Write-only logs, file permissions | ⚠️ File-based (consider centralized logging) |
| Privilege Escalation | Database whitelist, SELECT-only | ✅ Implemented |

## Known Limitations

### 1. Authentication
- **Current**: Network-level only (VPN + database permissions)
- **Limitation**: No per-user application-level tracking
- **Recommendation**: Acceptable for internal VPN deployment; implement SSO if needed for external access

### 2. Rate Limiting
- **Current**: Not implemented
- **Limitation**: Susceptible to query flooding from single user
- **Recommendation**: Implement per-user rate limiting if needed

### 3. Audit Log Storage
- **Current**: File-based with daily rotation
- **Limitation**: Local storage, manual retention management
- **Recommendation**: Forward to centralized logging (ELK, Splunk, CloudWatch)

### 4. Network Security
- **Current**: Relies on VPN network isolation
- **Limitation**: No application-level network filtering
- **Recommendation**: Acceptable for internal VPN deployment

### 5. Data Classification
- **Current**: No table/column-level access control
- **Limitation**: Users can query any table in allowed databases
- **Recommendation**: Implement if fine-grained access control is required

## Compliance Considerations

### Audit Requirements
✅ **Query Audit**: All queries logged with user, timestamp, database  
✅ **Access Audit**: Schema access operations logged  
✅ **PII Protection**: Sensitive data masked in logs  
⚠️ **Log Retention**: Application must implement retention policy

### Data Protection
✅ **Encryption in Transit**: Depends on database connection encryption  
✅ **Credential Protection**: Stored in AWS SSM with encryption  
✅ **Result Size Limits**: Prevents bulk data extraction

### Access Control
✅ **Authentication**: Network-level (VPN isolation)  
✅ **Authorization**: Database-level (SQL Server permissions)  
⚠️ **Fine-grained**: No application-level table/column restrictions

## Security Testing Recommendations

### Pre-Deployment Testing
1. **SQL Injection Testing**
   - Test malicious table names
   - Test query parameter injection
   - Test encoding bypass attempts

2. **Network Access Testing**
   - Verify VPN requirement for access
   - Test database permission enforcement
   - Verify connection from unauthorized networks fails

3. **Audit Logging Verification**
   - Verify all operations are logged
   - Verify PII sanitization works
   - Verify log file permissions

4. **Error Handling Testing**
   - Verify generic error messages
   - Verify no sensitive data in errors
   - Verify detailed errors only in logs

5. **Limit Testing**
   - Test query length limits
   - Test result row limits
   - Test timeout enforcement

### Post-Deployment Monitoring
1. Review audit logs daily for suspicious patterns
2. Monitor query execution times for anomalies
3. Correlate with VPN logs for user activity tracking
4. Track query patterns and unusual access
5. Review error rates and types

## Security Review Checklist

- [x] SQL Injection protection implemented and tested
- [x] Input validation for all user inputs
- [x] Query safety controls (SELECT-only)
- [x] Audit logging comprehensive and PII-safe
- [x] Error messages sanitized (no information disclosure)
- [x] Connection strings protected and never exposed
- [x] Network-level access control (VPN isolation)
- [x] Database whitelist enforced
- [x] Query complexity limits enforced
- [x] Result size limits enforced
- [x] Timeout controls implemented
- [x] Documentation complete and accurate
- [ ] External security penetration testing (recommended)
- [ ] Code review by security team (recommended)

## Recommendations for Cisco Review

### Immediate Deployment (Internal VPN)
✅ **Safe to deploy** with current security controls:
- Enable audit logging (`AUDIT_LOG_ENABLED=true`)
- VPN network isolation provides access control
- Database permissions control data access
- Review audit logs regularly
- Monitor for suspicious patterns

### External/Production Deployment
⚠️ **Current implementation NOT suitable for external access**:
- No application-level user authentication
- Designed for internal VPN trusted network only
- Would require significant changes for external deployment:
  1. Implement proper SSO authentication
  2. Add per-user authorization logic
  3. Configure centralized audit log forwarding
  4. Implement per-user rate limiting
  5. Conduct external security penetration testing

## Contact & Support

For security questions or incident response:
- Review audit logs: `./logs/audit/audit_YYYYMMDD.log`
- Check server logs: Output panel → "MCP: sql-server-mcp"
- Environment info: Call `get_environment_info()` tool

---

**Document Version**: 1.0  
**Last Updated**: October 28, 2025  
**Reviewed By**: [Pending]  
**Approved By**: [Pending]

