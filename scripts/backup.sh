#!/bin/bash

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/full_backup_${TIMESTAMP}.dump"

echo "📦 Starting backup to ${BACKUP_FILE}..."

mkdir -p "${BACKUP_DIR}"

# بکاپ از دیتابیس اصلی (gateway_db)
PGPASSWORD="${POSTGRES_PASSWORD}" pg_dump \
  -h localhost \
  -U "${POSTGRES_USER}" \
  -d "${POSTGRES_DB}" \
  -F c \
  -f "${BACKUP_FILE}"

if [ $? -eq 0 ]; then
  echo "✅ Backup successful: ${BACKUP_FILE}"
  
  # پاکسازی فایل‌های قدیمی‌تر از retention days
  echo "🧹 Cleaning backups older than ${BACKUP_RETENTION_DAYS} days..."
  find "${BACKUP_DIR}" -type f -name "*.dump" -mtime +${BACKUP_RETENTION_DAYS} -delete
  echo "✅ Cleanup completed."
else
  echo "❌ Backup failed!"
fi