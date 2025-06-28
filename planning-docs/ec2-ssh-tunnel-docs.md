# RDS Database Access Commands

## SSH Tunnel Setup

### 1. Open SSH Tunnel (Keep this terminal open!)
```bash
ssh -i terraform/bastion-key.pem -L 5434:fao-api-db.cloe4i4o0hs7.us-west-2.rds.amazonaws.com:5432 ec2-user@$(terraform output -raw bastion_public_ip) -N
```

Or with specific IP:
```bash
ssh -i terraform/bastion-key.pem -L 5434:fao-api-db.cloe4i4o0hs7.us-west-2.rds.amazonaws.com:5432 ec2-user@54.203.52.172 -N
```

Keep alive
```bash
ssh -i terraform/bastion-key.pem -L 5434:fao-api-db.cloe4i4o0hs7.us-west-2.rds.amazonaws.com:5432 ec2-user@54.203.52.172 -N -o ServerAliveInterval=60 -o ServerAliveCountMax=3
```


```bash
PGPASSWORD='your-password' psql -h localhost -p 5434 -U faoadmin -d fao -c "SELECT pg_size_pretty(pg_database_size('fao'));"

# Upload dump to bastion
scp -i terraform/bastion-key.pem fao_dump_20250617_123638.dump ec2-user@54.203.52.172:~/

# SSH into bastion
ssh -i terraform/bastion-key.pem ec2-user@54.203.52.172

# Install PostgreSQL client if needed
sudo apt update && sudo apt install -y postgresql-client

# Restore directly from bastion (no tunnel needed)
PGPASSWORD='your-password' pg_restore -h fao-api-db.cloe4i4o0hs7.us-west-2.rds.amazonaws.com -U faoadmin -d fao --verbose --no-owner --no-privileges --jobs 4 ~/fao_dump_20250617_123638.dump
```

**Note**: Keep this terminal window open while working!

## Database Connection Info

### Get RDS Password
```bash
aws ssm get-parameter --name "arn:aws:ssm:us-west-2:814437249083:parameter/fao-api/rds-password" --with-decryption --region us-west-2 --query 'Parameter.Value' --output text
```

### Connection Details
- **Host**: `localhost`
- **Port**: `5434`
- **Database**: `fao`
- **Username**: `faoadmin`
- **Password**: (from SSM command above)

## Connect to Database

### Using psql
```bash
# With password in environment
PGPASSWORD='your-password-here' psql -h localhost -p 5434 -U faoadmin -d fao

# Or let it prompt
psql -h localhost -p 5434 -U faoadmin -d fao
```

### Connection String
```
postgresql://faoadmin:your-password@localhost:5434/fao
```

### pgAdmin Setup
1. Host: `localhost`
2. Port: `5434`
3. Database: `fao`
4. Username: `faoadmin`
5. Password: (from SSM)

## Database Restore

### Full Restore Command
```bash
PGPASSWORD='your-password-here' pg_restore -h localhost -p 5434 -U faoadmin -d fao --verbose --no-owner --no-privileges --jobs 4 fao_dump_20250617_123638.dump
```

## Monitoring Queries

### Check Database Size
```bash
PGPASSWORD='your-password' psql -h localhost -p 5434 -U faoadmin -d fao -c "SELECT pg_size_pretty(pg_database_size('fao'));"
```

### List Tables by Size
```bash
PGPASSWORD='your-password' psql -h localhost -p 5434 -U faoadmin -d fao -c "
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'public' 
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC 
LIMIT 10;"
```

### Check Specific Table Count
```bash
PGPASSWORD='your-password' psql -h localhost -p 5434 -U faoadmin -d fao -c "SELECT COUNT(*) FROM trade_detailed_trade_matrix;"
```

## Troubleshooting

### If SSH Key Permission Error
```bash
chmod 600 terraform/bastion-key.pem
```

### If Port 5434 is Busy
Use a different port (e.g., 5434):
```bash
ssh -i terraform/bastion-key.pem -L 5434:fao-api-db.cloe4i4o0hs7.us-west-2.rds.amazonaws.com:5432 ec2-user@54.203.52.172 -N
```

### Get Bastion IP
```bash
terraform output bastion_public_ip
```

## Notes
- The SSH tunnel must stay open for database access
- Port 5434 on your local machine forwards to RDS port 5432
- The restore will show `transaction_timeout` errors - these are safe to ignore (PostgreSQL version mismatch)