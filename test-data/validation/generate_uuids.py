#!/usr/bin/env python3
import uuid

print("# Generated UUID v4 IDs for test data")
print("# Format: int_<uuid>, acc_<uuid>, usr_<uuid>")
print()

print("# TENANT_ACME - HEADQUARTERS")
print(f"acc_headquarters = 'acc_{uuid.uuid4()}'")
for i in range(3):
    print(f"usr_acme_hq_{i+1} = 'usr_{uuid.uuid4()}'")
print()

print("# TENANT_ACME - NYC BRANCH") 
print(f"acc_nyc_branch = 'acc_{uuid.uuid4()}'")
for i in range(2):
    print(f"usr_acme_nyc_{i+1} = 'usr_{uuid.uuid4()}'")
print()

print("# TENANT_BETA - MAIN")
print(f"acc_beta_main = 'acc_{uuid.uuid4()}'")
for i in range(2):
    print(f"usr_beta_{i+1} = 'usr_{uuid.uuid4()}'")
print()

print("# INTERACTION IDs")
for i in range(1, 17):
    print(f"int_{i:03d} = 'int_{uuid.uuid4()}'")
