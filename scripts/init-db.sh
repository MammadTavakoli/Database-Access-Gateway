#!/bin/bash
set -e

echo "🚀 Starting PostgreSQL Gateway Initialization..."

# ---------------------------------------------------------
# تنظیمات مربوط به فایل پایتون
# ---------------------------------------------------------
# لطفا نام فایل پایتون خود را در متغیر زیر وارد کنید
# اگر فایل وجود نداشته باشد، رد می‌شود (Skip) اما دیتابیس بالا می‌آید
PYTHON_SCRIPT_PATH="/app/scripts/provision.py"

# 1. شروع سرویس Cron
echo "🕒 Starting Cron Service..."
service cron start

# 2. تضمین وجود دایرکتوری سوکت
mkdir -p /var/run/postgresql
chown postgres:postgres /var/run/postgresql
chmod 775 /var/run/postgresql

# 3. اجرای دیتابیس در پس‌زمینه (Background)
echo "⏳ Starting PostgreSQL in background..."
docker-entrypoint.sh postgres &
POSTGRES_PID=$!

# 4. صبر کردن تا زمانی که دیتابیس کاملاً آماده شود
sleep 2

echo "⏳ Waiting for PostgreSQL to accept connections..."
until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"; do
  echo "Waiting for database... (sleep 1s)"
  sleep 1
done
echo "✅ PostgreSQL is ready."

# 5. اجرای فایل پایتون
if [ -f "$PYTHON_SCRIPT_PATH" ]; then
    echo "🐍 Running Python initialization script: $PYTHON_SCRIPT_PATH ..."
    # استفاده از gosu برای اجرا با دسترسی کاربر postgres
    gosu postgres python3 "$PYTHON_SCRIPT_PATH"
    echo "✅ Python script executed successfully."
else
    echo "⚠️  Python script not found at: $PYTHON_SCRIPT_PATH (Skipping initialization logic)"
fi

# 6. متوقف کردن دیتابیس پس‌زمینه
echo "🛑 Stopping temporary PostgreSQL process..."
# استفاده از gosu برای توقف تمیز دیتابیس
gosu postgres pg_ctl -D "$PGDATA" -m fast stop
# منتظر ماندن برای خروج کامل پروسه
wait $POSTGRES_PID 2>/dev/null || true

# 7. اجرای نهایی دیتابیس در حالت پیش‌زمینه (Foreground)
echo "🚀 Starting PostgreSQL in foreground mode..."
exec docker-entrypoint.sh postgres