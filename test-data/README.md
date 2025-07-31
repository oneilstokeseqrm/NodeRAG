# EQ Test Data Suite

This directory contains comprehensive test data for the NodeRAG EQ (Enterprise Quality) project, specifically designed to validate metadata propagation and multi-tenant isolation capabilities.

## 📁 Directory Structure

```
test-data/
├── README.md                          # This documentation file
├── sample-interactions/               # Test interaction data organized by tenant
│   ├── tenant_acme/                  # ACME Corporation test tenant
│   │   ├── account_headquarters/     # Headquarters account interactions
│   │   │   ├── interaction_001.json  # Email - Invoice discrepancy
│   │   │   ├── interaction_002.json  # Call - License renewal
│   │   │   ├── interaction_003.json  # Chat - Technical support
│   │   │   ├── interaction_004.json  # Voice memo - Meeting notes
│   │   │   └── interaction_005.json  # Custom notes - Account review
│   │   └── account_branch_nyc/       # NYC Branch account interactions
│   │       ├── interaction_006.json  # Email - User onboarding
│   │       ├── interaction_007.json  # Call - Account setup
│   │       ├── interaction_008.json  # Chat - Feature request
│   │       ├── interaction_009.json  # Voice memo - Team update
│   │       └── interaction_010.json  # Custom notes - Performance review
│   └── tenant_beta/                  # Beta Corporation test tenant
│       └── account_main/             # Main account interactions
│           ├── interaction_011.json  # Email - Integration requirements
│           ├── interaction_012.json  # Call - Technical discovery
│           ├── interaction_013.json  # Chat - Implementation support
│           ├── interaction_014.json  # Voice memo - Planning session
│           ├── interaction_015.json  # Custom notes - Project status
│           └── interaction_016.json  # Email - Pilot results
├── validation/                       # Validation scripts and tools
│   └── validate_test_data.py        # Main validation script
└── expected-outputs/                 # Generated validation reports
    ├── test_data_validation_report.html  # HTML validation report
    └── test_data_statistics.csv         # CSV statistics summary
```

## 🏗️ Data Structure

Each interaction file contains exactly **8 required metadata fields**:

### Required Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `tenant_id` | String | Tenant identifier (test format) | `"tenant_acme"` |
| `interaction_id` | UUID v4 | Unique interaction identifier | `"int_6ba7b810-9dad-11d1-80b4-00c04fd430c8"` |
| `interaction_type` | Enum | Type of customer interaction | `"email"` |
| `text` | String | Full interaction content/transcript | `"Subject: Invoice Question..."` |
| `account_id` | UUID v4 | Account identifier within tenant | `"acc_6ba7b811-9dad-11d1-80b4-00c04fd430c8"` |
| `timestamp` | ISO8601 | Interaction timestamp in UTC | `"2024-01-15T10:30:00Z"` |
| `user_id` | UUID v4 | User/agent identifier | `"usr_6ba7b812-9dad-11d1-80b4-00c04fd430c8"` |
| `source_system` | Enum | System that captured the interaction | `"outlook"` |

### Field Validation Rules

#### ID Standards
- **UUID v4 Format**: All IDs (except tenant_id) must follow UUID v4 format
- **Prefixed UUIDs**: IDs use prefixes for easier identification:
  - `int_` for interaction_id
  - `acc_` for account_id  
  - `usr_` for user_id
- **Test Tenant IDs**: Human-readable format for testing clarity:
  - `tenant_acme` (ACME Corporation)
  - `tenant_beta` (Beta Corporation)

#### Enumerated Values

**interaction_type** (valid values):
- `call` - Phone call transcripts
- `chat` - Live chat sessions  
- `email` - Email communications
- `voice_memo` - Voice recordings/memos
- `custom_notes` - Manual notes/entries

**source_system** (valid values):
- `internal` - Internal EQ platform entries
- `voice_memo` - Voice recording ingestion
- `custom` - Custom API/webhook ingestion
- `outlook` - Outlook email integration
- `gmail` - Gmail integration

#### Content Requirements
- **text**: Must contain meaningful business content (minimum 10 characters)
- **timestamp**: Must be valid ISO8601 format in UTC timezone
- **All fields**: Must be non-empty and contain valid data

## 🎯 Test Scenarios

### Multi-Tenant Isolation
- **tenant_acme**: 10 interactions across 2 accounts (headquarters, NYC branch)
- **tenant_beta**: 6 interactions across 1 account (main)
- Tests ensure data isolation between tenants

### Interaction Type Coverage
- **Email**: 4 interactions (billing, onboarding, integration, results)
- **Call**: 3 interactions (support, setup, discovery)  
- **Chat**: 3 interactions (technical, feature request, implementation)
- **Voice Memo**: 3 interactions (meeting notes, updates, planning)
- **Custom Notes**: 3 interactions (reviews, status, performance)

### Source System Coverage
- **internal**: 6 interactions (platform-native entries)
- **outlook**: 2 interactions (Outlook email integration)
- **gmail**: 2 interactions (Gmail integration)
- **voice_memo**: 3 interactions (voice recording system)
- **custom**: 3 interactions (API/webhook ingestion)

### Business Context Scenarios
- **Customer Support**: Technical issues, billing questions, account setup
- **Sales & Account Management**: Renewals, upgrades, relationship management
- **Integration Projects**: Technical discovery, implementation, pilot testing
- **Internal Operations**: Team updates, performance reviews, planning sessions

## 🔍 Validation

### Automated Validation Script

Run the validation script to verify all test data:

```bash
cd test-data/validation
python validate_test_data.py
```

### Validation Checks

The script performs comprehensive validation:

1. **Field Presence**: All 8 required fields must be present
2. **Field Content**: No empty or whitespace-only values
3. **UUID Format**: All IDs follow UUID v4 format with correct prefixes
4. **Enum Values**: interaction_type and source_system use valid values only
5. **Timestamp Format**: ISO8601 format validation with timezone
6. **Content Quality**: Meaningful text content (minimum length)
7. **Tenant Isolation**: Proper tenant_id usage

### Generated Reports

After validation, two reports are generated:

1. **HTML Report** (`expected-outputs/test_data_validation_report.html`)
   - Visual validation summary with pass/fail status
   - Detailed error listing if validation fails
   - Data distribution statistics
   - Professional formatting for stakeholder review

2. **CSV Statistics** (`expected-outputs/test_data_statistics.csv`)
   - Machine-readable statistics for automated processing
   - Distribution by tenant, interaction type, source system, and account
   - Suitable for data analysis and reporting tools

## 📊 Data Statistics

### Current Test Data Volume
- **Total Interactions**: 16
- **Tenants**: 2 (tenant_acme, tenant_beta)
- **Accounts**: 3 (2 for ACME, 1 for Beta)
- **Time Range**: January 2024 - February 2024

### Distribution Summary
- **By Tenant**: ACME (10), Beta (6)
- **By Type**: Email (4), Call (3), Chat (3), Voice Memo (3), Custom Notes (3)
- **By Source**: Internal (6), Voice Memo (3), Custom (3), Outlook (2), Gmail (2)

## 🚀 Usage Guidelines

### For Development Testing
1. Use this test data to validate metadata propagation in NodeRAG
2. Test multi-tenant isolation by querying across tenant boundaries
3. Verify that all 8 metadata fields are preserved through processing
4. Validate search and retrieval functionality with realistic business content

### For Integration Testing
1. Import test data into NodeRAG build pipeline
2. Verify graph construction preserves metadata relationships
3. Test search functionality across different interaction types
4. Validate tenant-specific data access controls

### For Performance Testing
1. Scale test data volume by duplicating and modifying interactions
2. Maintain the same field structure and validation rules
3. Update tenant_id and account_id values for additional test scenarios
4. Re-run validation after any modifications

## 🔒 Security & Privacy

### Data Safety
- **No Real Data**: All interactions contain fictional business scenarios
- **No PII**: No personally identifiable information included
- **Test-Only IDs**: All identifiers are generated for testing purposes
- **Safe Content**: All text content is appropriate for development environments

### Compliance Notes
- Test data follows enterprise data structure standards
- Metadata fields support audit trail requirements
- Tenant isolation design supports multi-tenant compliance
- Timestamp precision supports regulatory reporting needs

## 🛠️ Maintenance

### Adding New Test Data
1. Follow the established JSON structure exactly
2. Use the validation script to verify new data
3. Maintain realistic business content
4. Update this README if adding new scenarios

### Modifying Existing Data
1. Preserve the 8 required metadata fields
2. Maintain UUID v4 format for all IDs
3. Keep enumerated values within valid sets
4. Re-run validation after changes

### Version Control
- All test data is version controlled with the NodeRAG repository
- Changes should be made via pull requests
- Validation must pass before merging changes
- Document significant changes in commit messages

## 📞 Support

For questions about the test data structure or validation:
1. Review this README for comprehensive documentation
2. Run the validation script for specific error details
3. Check the generated HTML report for visual validation results
4. Refer to the NodeRAG project documentation for integration guidance

---

**Generated for NodeRAG EQ Project - Phase 1: Foundation Setup**  
**Last Updated**: January 2024  
**Validation Status**: ✅ All checks passing
