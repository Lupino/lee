from lee.conf import use_mysql
if use_mysql:
    from .oursql import query, create_table, show_tables, diff_table
else:
    from .sqlite import query, create_table, show_tables, diff_table
