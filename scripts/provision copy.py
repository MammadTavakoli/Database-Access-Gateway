import os
import psycopg2
from psycopg2 import sql
import yaml
from pathlib import Path
from collections import defaultdict

# ==========================================
# تنظیمات سراسری
# ==========================================
DB_ADMIN_USER = os.environ.get('POSTGRES_USER')
DB_ADMIN_PASS = os.environ.get('POSTGRES_PASSWORD')
DB_DEFAULT_NAME = os.environ.get('POSTGRES_DB')

def connect(db='postgres'):
    """ایجاد اتصال به دیتابیس"""
    try:
        conn = psycopg2.connect(
            host='localhost', port='5432',
            user=DB_ADMIN_USER,
            password=DB_ADMIN_PASS,
            database=db
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"❌ Connection failed to {db}: {e}")
        raise

def resolve_global_env(val):
    """حل کردن متغیرهای محیطی ساده"""
    if not isinstance(val, str): return val
    if val.startswith('${') and val.endswith('}'):
        env_key = val[2:-1]
        resolved = os.environ.get(env_key)
        if not resolved:
            return val
        return resolved
    return val

def resolve_config_val(val, context_vars):
    """حل کردن متغیرها با اولویت متغیرهای مسیر و سپس محیطی"""
    if not isinstance(val, str): return val
    
    for key, value in context_vars.items():
        val = val.replace(f"${{{key}}}", value)
        
    if val.startswith('${') and val.endswith('}'):
        env_key = val[2:-1]
        resolved = os.environ.get(env_key, val)
        return resolved
    return val

def parse_identifier(name_str):
    """تبدیل 'schema.table' به sql.Identifier"""
    if not name_str: return None
    parts = name_str.split('.')
    return sql.Identifier(*parts)

# ==========================================
# توابع زیرساخت امنیتی
# ==========================================

def setup_security_infrastructure(conn_db):
    cur = conn_db.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS auth_policies (
            username VARCHAR(255) PRIMARY KEY, 
            allowed_start TIME, 
            allowed_end TIME, 
            description TEXT
        );
    """)
    cur.execute("""
        CREATE OR REPLACE FUNCTION enforce_access_policy() RETURNS VOID AS $$         DECLARE rec RECORD; 
        BEGIN
            SELECT * INTO rec FROM auth_policies WHERE username = current_user;
            IF FOUND AND rec.allowed_start IS NOT NULL AND rec.allowed_end IS NOT NULL THEN
                IF current_time < rec.allowed_start OR current_time > rec.allowed_end THEN
                    RAISE EXCEPTION 'Access DENIED for % outside allowed hours', current_user;
                END IF;
            END IF;
        END; $$ LANGUAGE plpgsql;
    """)
    cur.close()

def generate_filter_logic(filter_columns):
    """تولید کد PL/pgSQL برای اضافه کردن شرط‌های تساوی به کوئری داینامیک"""
    logic_lines = []
    for fc in filter_columns:
        fc_name = fc['name']
        logic_lines.append(f"""
        IF p_{fc_name} IS NOT NULL THEN
            v_query := v_query || ' AND {fc_name} = ' || quote_literal(p_{fc_name});
        END IF;
        """)
    return "\n".join(logic_lines)

# ==========================================
# توابع افزایشی (Incremental Jobs)
# ==========================================

def create_incremental_job_functions(conn_db, jobs_config, context_vars):
    """
    ایجاد توابع افزایشی (Windowing) با قابلیت فیلتر.
    """
    cur = conn_db.cursor()
    for job in jobs_config:
        func_name_str = resolve_config_val(job['name'], context_vars)
        target_table_str = resolve_config_val(job['target_table'], context_vars)
        key_column = job['key_column']
        key_type_raw = job['key_type']
        key_type = key_type_raw.lower()
        
        batch_size = job.get('batch_size', 1000)
        max_limit = job.get('max_limit', batch_size * 10) 
        filter_columns = job.get('filter_columns', [])
        
        default_val = "0"
        declare_section = ""
        logic_section = ""
        
        func_args_list = [
            f"p_last_{key_column} {key_type_raw} DEFAULT {default_val}",
            "p_limit INTEGER DEFAULT {}".format(batch_size)
        ]
        
        for fc in filter_columns:
            fc_name = fc['name']
            fc_type = fc['type']
            func_args_list.append(f"p_{fc_name} {fc_type} DEFAULT NULL")
        
        func_args_sql = ", ".join(func_args_list)
        
        if 'int' in key_type or 'num' in key_type or 'serial' in key_type:
            default_val = "0"
            declare_section = f"""
                v_limit INTEGER;
                v_start_val BIGINT;
                v_end_val BIGINT;
                v_query TEXT;
                r RECORD;
            """
            logic_section = f"""
                v_limit := LEAST(COALESCE(p_limit, {batch_size}), {max_limit});
                v_start_val := p_last_{key_column};
                v_end_val := p_last_{key_column} + v_limit;
                
                v_query := 'SELECT * FROM {target_table_str} ' ||
                           'WHERE {key_column} > ' || v_start_val::text || 
                           ' AND {key_column} <= ' || v_end_val::text;
                           
                {generate_filter_logic(filter_columns)}
                           
                FOR r IN EXECUTE v_query LOOP
                    RETURN NEXT r;
                END LOOP;
            """
            
        elif 'time' in key_type or 'date' in key_type:
            default_val = "'1900-01-01 00:00:00'::timestamp"
            declare_section = f"""
                v_limit INTEGER;
                v_start_val TIMESTAMP;
                v_end_val TIMESTAMP;
                v_query TEXT;
                r RECORD;
            """
            logic_section = f"""
                v_limit := LEAST(COALESCE(p_limit, {batch_size}), {max_limit});
                v_start_val := p_last_{key_column};
                v_end_val := p_last_{key_column} + make_interval(days => v_limit);
                
                v_query := 'SELECT * FROM {target_table_str} ' ||
                           'WHERE {key_column} > ' || quote_literal(v_start_val) || 
                           ' AND {key_column} <= ' || quote_literal(v_end_val);
                           
                {generate_filter_logic(filter_columns)}
                           
                FOR r IN EXECUTE v_query LOOP
                    RETURN NEXT r;
                END LOOP;
            """
        else:
            print(f"      ⚠️ Key type '{key_type}' not supported.")
            continue

        func_sql = f"""
        CREATE OR REPLACE FUNCTION {func_name_str}(
            {func_args_sql}
        )
        RETURNS SETOF {target_table_str} 
        LANGUAGE plpgsql 
        SECURITY DEFINER 
        AS $$         DECLARE
            {declare_section}
        BEGIN
            {logic_section}
            RETURN;
        END;
        $$;
        """
        
        try:
            cur.execute(func_sql)
            print(f"      ⚙️  Created Incremental Function: {func_name_str}")
        except Exception as e:
            print(f"      ❌ Function Error: {e}")
    cur.close()

# ==========================================
# توابع سفارشی (Custom Functions - Aggregation/Lookup)
# ==========================================

def create_custom_functions(conn_db, functions_config, context_vars):
    """
    ایجاد توابع سفارشی برای تجمیع (count, max) یا بازگشت ستون‌های خاص.
    """
    cur = conn_db.cursor()
    for func in functions_config:
        func_name_str = resolve_config_val(func['name'], context_vars)
        target_table_str = resolve_config_val(func['target_table'], context_vars)
        
        # بررسی وجود تعریف خروجی‌ها
        return_columns = func.get('return_columns', [])
        if not return_columns:
            print(f"      ⚠️ Skipping {func_name_str}: 'return_columns' not defined.")
            continue
            
        # ساخت بخش RETURNS TABLE (col1 type, col2 type)
        ret_def_list = [f"{rc['name']} {rc['type']}" for rc in return_columns]
        ret_def_sql = ", ".join(ret_def_list)
        
        # ساخت بخش SELECT (expr AS name, ...)
        select_expr_list = [f"{rc['expression']} AS {rc['name']}" for rc in return_columns]
        select_expr_sql = ", ".join(select_expr_list)
        
        # ساخت آرگومان‌های ورودی (فقط فیلترها)
        filter_columns = func.get('filter_columns', [])
        args_list = [f"p_{fc['name']} {fc['type']} DEFAULT NULL" for fc in filter_columns]
        args_sql = ", ".join(args_list)
        
        # تولید منطق فیلتر
        filter_logic = generate_filter_logic(filter_columns)
        
        # ساخت تابع نهایی
        # نکته: از RETURNS TABLE استفاده می‌شود تا خروجی مشخص باشد.
        func_sql = f"""
        CREATE OR REPLACE FUNCTION {func_name_str}({args_sql})
        RETURNS TABLE({ret_def_sql})
        LANGUAGE plpgsql
        SECURITY DEFINER
        AS $$         DECLARE
            v_query TEXT;
        BEGIN
            -- کوئری پایه با عبارات انتخابی خاص
            v_query := 'SELECT {select_expr_sql} FROM {target_table_str} WHERE 1=1';
            
            -- اضافه کردن فیلترها
            {filter_logic}
            
            -- اجرا و بازگشت نتیجه
            RETURN QUERY EXECUTE v_query;
        END;
        $$;
        """
        
        try:
            cur.execute(func_sql)
            print(f"      ⚙️  Created Custom Function: {func_name_str}")
        except Exception as e:
            print(f"      ❌ Custom Function Error {func_name_str}: {e}")
    cur.close()

# ==========================================
# منطق اصلی پردازش
# ==========================================

def create_global_users(all_users_list):
    """ایجاد تمام کاربران در سطح کلستر"""
    if not all_users_list: return
    
    print("👥 Creating Global Users (Roles)...")
    conn_glob = connect(DB_DEFAULT_NAME)
    cur = conn_glob.cursor()
    
    created_set = set()
    
    for user_data in all_users_list:
        username = resolve_global_env(user_data.get('username'))
        password = resolve_global_env(user_data.get('password'))
        
        if not username or not password: continue
        if username in created_set: continue
        
        try:
            cur.execute(sql.SQL("CREATE USER {} WITH PASSWORD %s;").format(sql.Identifier(username)), (password,))
            print(f"   ✅ Created user: {username}")
            created_set.add(username)
        except psycopg2.errors.DuplicateObject:
            try:
                cur.execute(sql.SQL("ALTER USER {} WITH PASSWORD %s;").format(sql.Identifier(username)), (password,))
                print(f"   ℹ️  User exists, updated password: {username}")
            except: pass
            created_set.add(username)
        except Exception as e:
            print(f"   ❌ User creation error ({username}): {e}")
            
    conn_glob.close()

def scan_and_collect_users(config_dir):
    """اسکن پوشه‌های user و users برای جمع‌آوری لیست کاربران"""
    users_list = []
    for folder_name in ['users', 'user']:
        for u_file in config_dir.rglob(f"{folder_name}/*.yaml"):
            try:
                with open(u_file, 'r') as f:
                    user_cfg = yaml.safe_load(f)
                    if user_cfg and (user_cfg.get('username') or user_cfg.get('name')):
                        users_list.append({
                            'username': resolve_global_env(user_cfg.get('username', user_cfg.get('name'))),
                            'password': resolve_global_env(user_cfg.get('password'))
                        })
            except: pass
    return users_list

def process_single_database(db_name_resolved, db_data, context_vars):
    """پردازش کامل یک دیتابیس"""
    
    print(f"\n   {'='*15} DATABASE: {db_name_resolved} {'='*15}")
    
    # 1. ایجاد دیتابیس
    conn_admin = connect(DB_DEFAULT_NAME)
    db_owner = DB_ADMIN_USER
    for cfg in db_data['configs']:
        if 'database' in cfg and 'owner' in cfg['database']:
            db_owner = resolve_config_val(cfg['database']['owner'], context_vars)
            break

    try:
        conn_admin.cursor().execute(sql.SQL("CREATE DATABASE {} OWNER {};").format(sql.Identifier(db_name_resolved), sql.Identifier(db_owner)))
        print("      ✅ Database created.")
    except psycopg2.errors.DuplicateDatabase:
        print("      ℹ️  Database exists.")
    except Exception as e:
        print(f"      ❌ DB Error: {e}")
        conn_admin.close()
        return
    finally:
        conn_admin.close()

    conn_db = connect(db_name_resolved)
    cur = conn_db.cursor()

    # Extensions
    for ext in ['mysql_fdw', 'tds_fdw', 'pg_stat_statements']:
        try: cur.execute(sql.SQL("CREATE EXTENSION IF NOT EXISTS {};").format(sql.Identifier(ext)))
        except: pass
    
    setup_security_infrastructure(conn_db)

    # 2. تجیع داده‌ها
    all_schemas = []
    all_fdws = []
    all_tables = []
    all_views = []
    all_jobs = []
    all_custom_funcs = [] # 🟢 اضافه شدن لیست توابع سفارشی
    all_permissions = []
    
    schema_names_set = set()
    fdw_names_set = set()
    table_names_set = set()
    view_names_set = set()
    job_names_set = set()
    custom_func_names_set = set() # 🟢

    for cfg in db_data['configs']:
        for s in cfg.get('schemas', []):
            s_name = resolve_config_val(s['name'], context_vars)
            if s_name not in schema_names_set:
                all_schemas.append(s); schema_names_set.add(s_name)
        
        for f in cfg.get('fdws', []):
            f_name = resolve_config_val(f['name'], context_vars)
            if f_name not in fdw_names_set:
                all_fdws.append(f); fdw_names_set.add(f_name)
                
        for t in cfg.get('tables', []):
            t_name = resolve_config_val(t['name'], context_vars)
            if t_name not in table_names_set:
                all_tables.append(t); table_names_set.add(t_name)
                
        for v in cfg.get('views', []):
            v_name = resolve_config_val(v['name'], context_vars)
            if v_name not in view_names_set:
                all_views.append(v); view_names_set.add(v_name)
                
        for j in cfg.get('incremental_jobs', []):
            j_name = resolve_config_val(j['name'], context_vars)
            if j_name not in job_names_set:
                all_jobs.append(j); job_names_set.add(j_name)
        
        # 🟢 جمع‌آوری توابع سفارشی
        for cf in cfg.get('custom_functions', []):
            cf_name = resolve_config_val(cf['name'], context_vars)
            if cf_name not in custom_func_names_set:
                all_custom_funcs.append(cf); custom_func_names_set.add(cf_name)

    for perm_cfg in db_data['user_permissions']:
        all_permissions.append(perm_cfg)

    # 3. اجرای دستورات

    # Schemas
    print("      🏗️  Schemas...")
    for s in all_schemas:
        s_name = resolve_config_val(s['name'], context_vars)
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(s_name)))

    # FDWs
    print("      🌐 FDWs...")
    fdw_credentials = {}
    
    for fdw in all_fdws:
        fdw_name = resolve_config_val(fdw['name'], context_vars)
        fdw_type = fdw['type']
        options = {}
        
        if fdw_type == 'tds':
            if 'config_section' in fdw:
                options['servername'] = fdw['config_section']
                print(f"         🔗 Using freetds.conf section: {fdw['config_section']}")
                if 'database' in fdw: options['database'] = resolve_config_val(fdw['database'], context_vars)
            else:
                host_val = resolve_config_val(fdw.get('host'), context_vars)
                options['servername'] = host_val
                options['port'] = str(resolve_config_val(fdw.get('port'), context_vars))
                print(f"         🔗 Using direct connection: {host_val}")
                if 'database' in fdw: options['database'] = resolve_config_val(fdw['database'], context_vars)
            
            options['tds_version'] = fdw.get('tds_version', '7.4')

        elif fdw_type == 'mysql':
            options['host'] = resolve_config_val(fdw.get('host'), context_vars)
            options['port'] = str(resolve_config_val(fdw.get('port'), context_vars))
            if 'database' in fdw: options['dbname'] = resolve_config_val(fdw['database'], context_vars)

        opts_sql = sql.SQL(', ').join([sql.SQL("{} {}").format(sql.Identifier(k), sql.Literal(v)) for k,v in options.items() if v])
        
        try: 
            cur.execute(sql.SQL("CREATE SERVER {} FOREIGN DATA WRAPPER {} OPTIONS ({});").format(
                sql.Identifier(fdw_name), sql.Identifier(f"{fdw_type}_fdw"), opts_sql))
        except psycopg2.errors.DuplicateObject: 
            pass
        
        fdw_credentials[fdw_name] = {
            'user': resolve_config_val(fdw['user'], context_vars),
            'password': resolve_config_val(fdw['password'], context_vars)
        }

    # Tables
    print("      📊 Tables...")
    for tbl in all_tables:
        tbl_name = resolve_config_val(tbl['name'], context_vars)
        server_name = resolve_config_val(tbl['server'], context_vars)
        cols = sql.SQL(', ').join([sql.SQL("{} {}").format(sql.Identifier(c['name']), sql.SQL(c['type'])) for c in tbl['columns']])
        
        opts = []
        if 'remote_schema' in tbl: opts.append(sql.SQL("schema_name {}").format(sql.Literal(tbl['remote_schema'])))
        if 'remote_table' in tbl: opts.append(sql.SQL("table_name {}").format(sql.Literal(tbl['remote_table'])))
        opts_sql = sql.SQL(', ').join(opts)
        
        try: 
            cur.execute(sql.SQL("CREATE FOREIGN TABLE {} ({}) SERVER {} OPTIONS ({});").format(parse_identifier(tbl_name), cols, sql.Identifier(server_name), opts_sql))
            print(f"         ✅ Created table: {tbl_name}")
        except psycopg2.errors.DuplicateTable: 
            print(f"         ℹ️  Table exists: {tbl_name}")
        except Exception as e: 
            print(f"      ❌ Table Error {tbl_name}: {e}")

    # Views
    print("      👁️  Views...")
    for vw in all_views:
        vw_name = resolve_config_val(vw['name'], context_vars)
        vw_sql = resolve_config_val(vw['sql'], context_vars)
        try: 
            cur.execute(sql.SQL("CREATE OR REPLACE VIEW {} AS {};").format(parse_identifier(vw_name), sql.SQL(vw_sql)))
        except Exception as e: 
            print(f"      ❌ View Error {vw_name}: {e}")

    # Incremental JOBS
    if all_jobs:
        create_incremental_job_functions(conn_db, all_jobs, context_vars)
    
    # 🟢 Custom Functions
    if all_custom_funcs:
        create_custom_functions(conn_db, all_custom_funcs, context_vars)

    # 4. مدیریت کاربران و دسترسی‌ها
    print("      🔐 Permissions...")
    
    users_in_this_db = []

    for user in all_permissions:
        username = resolve_config_val(user.get('username'), context_vars)
        users_in_this_db.append(username)

        cur.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {};").format(sql.Identifier(db_name_resolved), sql.Identifier(username)))
        
        for s_name in schema_names_set:
            cur.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {};").format(sql.Identifier(s_name), sql.Identifier(username)))

        access_time = user.get('access_time')
        if access_time: 
            st = access_time.get('start')
            en = access_time.get('end')
            if st and en: 
                cur.execute("INSERT INTO auth_policies (username, allowed_start, allowed_end) VALUES (%s,%s,%s) ON CONFLICT (username) DO UPDATE SET allowed_start=%s, allowed_end=%s;", 
                           (username, st, en, st, en))

        for perm in user.get('permissions', []):
            if 'view' in perm:
                v_conf = perm['view']
                v_name = None
                cols = None
                if isinstance(v_conf, str): v_name = resolve_config_val(v_conf, context_vars)
                elif isinstance(v_conf, dict):
                    v_name = resolve_config_val(v_conf.get('name'), context_vars)
                    cols = v_conf.get('columns')
                if v_name:
                    try:
                        if cols:
                            cols_sql = sql.SQL(', ').join([sql.Identifier(c) for c in cols])
                            cur.execute(sql.SQL("GRANT SELECT ({}) ON {} TO {};").format(cols_sql, parse_identifier(v_name), sql.Identifier(username)))
                        else:
                            cur.execute(sql.SQL("GRANT SELECT ON {} TO {};").format(parse_identifier(v_name), sql.Identifier(username)))
                    except: pass
            if 'table' in perm:
                t_conf = perm['table']
                t_name = None
                if isinstance(t_conf, str): t_name = resolve_config_val(t_conf, context_vars)
                elif isinstance(t_conf, dict): t_name = resolve_config_val(t_conf.get('name'), context_vars)
                if t_name:
                    try: cur.execute(sql.SQL("GRANT SELECT ON {} TO {};").format(parse_identifier(t_name), sql.Identifier(username)))
                    except: pass
            if 'function' in perm:
                f_conf = perm['function']
                f_name = None
                if isinstance(f_conf, str): f_name = resolve_config_val(f_conf, context_vars)
                elif isinstance(f_conf, dict): f_name = resolve_config_val(f_conf.get('name'), context_vars)
                if f_name:
                    try: cur.execute(sql.SQL("GRANT EXECUTE ON FUNCTION {} TO {};").format(parse_identifier(f_name), sql.Identifier(username)))
                    except: pass

    # Grant execute for jobs
    for job in all_jobs:
        func_name_str = resolve_config_val(job['name'], context_vars)
        for consumer in job.get('allowed_consumers', []):
            resolved_user = resolve_config_val(consumer, context_vars)
            try: cur.execute(sql.SQL("GRANT EXECUTE ON FUNCTION {} TO {};").format(parse_identifier(func_name_str), sql.Identifier(resolved_user)))
            except: pass
            
    # 🟢 Grant execute for custom functions
    for cf in all_custom_funcs:
        func_name_str = resolve_config_val(cf['name'], context_vars)
        for consumer in cf.get('allowed_consumers', []):
            resolved_user = resolve_config_val(consumer, context_vars)
            try: cur.execute(sql.SQL("GRANT EXECUTE ON FUNCTION {} TO {};").format(parse_identifier(func_name_str), sql.Identifier(resolved_user)))
            except: pass

    # User Mappings
    print("      🔗 Creating User Mappings...")
    for fdw_name in fdw_credentials:
        try: 
            cur.execute(sql.SQL("CREATE USER MAPPING FOR {} SERVER {} OPTIONS (username %s, password %s);").format(
                sql.Identifier(DB_ADMIN_USER), sql.Identifier(fdw_name)), 
                (fdw_credentials[fdw_name]['user'], fdw_credentials[fdw_name]['password']))
            print(f"         🔑 Mapped admin '{DB_ADMIN_USER}' to server {fdw_name}")
        except psycopg2.errors.DuplicateObject: 
            pass
        except Exception as e:
            print(f"         ⚠️ Admin mapping error: {e}")

        for l_user in users_in_this_db:
            try: 
                cur.execute(sql.SQL("CREATE USER MAPPING FOR {} SERVER {} OPTIONS (username %s, password %s);").format(
                    sql.Identifier(l_user), sql.Identifier(fdw_name)), 
                    (fdw_credentials[fdw_name]['user'], fdw_credentials[fdw_name]['password']))
            except: pass

    conn_db.close()
    print(f"      ✅ Database {db_name_resolved} Processing Complete.")

def process_domain(domain_path):
    parent_dir = domain_path.parent.name
    current_dir = domain_path.name
    context_vars = {'__parent__': parent_dir, '__current__': current_dir}

    print(f"\n{'='*20} PROCESSING DOMAIN: {parent_dir}/{current_dir} {'='*20}")

    db_configs_map = defaultdict(lambda: {"configs": [], "user_permissions": []})

    for y_file in domain_path.glob("*.yaml"):
        try:
            with open(y_file, 'r') as f:
                cfg = yaml.safe_load(f)
                if not cfg: continue
                if 'database' in cfg:
                    db_name = resolve_config_val(cfg['database']['name'], context_vars)
                    db_configs_map[db_name]['configs'].append(cfg)
        except Exception as e:
            print(f"      ❌ Error reading {y_file.name}: {e}")

    users_path = domain_path / "users"
    if not users_path.exists():
        users_path = domain_path / "user"
        
    if users_path.exists():
        for u_file in users_path.glob("*.yaml"):
            try:
                with open(u_file, 'r') as f:
                    user_cfg = yaml.safe_load(f)
                    if not user_cfg: continue
                    
                    for grant in user_cfg.get('grants', []):
                        target_db = resolve_config_val(grant.get('database'), context_vars)
                        if target_db:
                            user_data = {
                                'username': resolve_config_val(user_cfg.get('username', user_cfg.get('name')), context_vars),
                                'access_time': grant.get('access_time', user_cfg.get('access_time')),
                                'permissions': grant.get('permissions', [])
                            }
                            db_configs_map[target_db]['user_permissions'].append(user_data)
            except Exception as e:
                print(f"      ❌ Error reading user file {u_file.name}: {e}")

    for db_name, data in db_configs_map.items():
        process_single_database(db_name, data, context_vars)

def main():
    print("🚀 Starting Modular Provisioning Engine...")
    config_dir = Path("/app/configs/domains")
    
    all_users = scan_and_collect_users(config_dir)
    create_global_users(all_users)
    
    if config_dir.exists():
        for project_folder in config_dir.iterdir():
            if project_folder.is_dir():
                has_subfolders = any(f.is_dir() for f in project_folder.iterdir() if f.name not in ['user', 'users'])
                
                if has_subfolders:
                    for domain_folder in project_folder.iterdir():
                        if domain_folder.is_dir() and domain_folder.name not in ['user', 'users']:
                            process_domain(domain_folder)
                else:
                    process_domain(project_folder)

    print("\n🎉 ALL TASKS COMPLETED.")
    print("ℹ️  IMPORTANT: Connect to the specific database to query tables.")

if __name__ == "__main__": main()