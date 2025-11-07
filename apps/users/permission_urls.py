# apps/users/permission_urls.py

PERMISSION_URLS = {
    # -----------------------
    # Leave
    # -----------------------
    "leave_apply": "leave:apply_leave",
    "leave_list": "leave:my_leaves",
    "leave_pending_manager": "leave:pending_leaves",  # optional, if used
    "leave_pending_hr": "leave:hr_leaves",            # optional, if used
    # Admin CC management (UI convenience; views still enforce superuser)
    "leave_cc_admin": "leave:cc_config",

    # -----------------------
    # Checklist
    # -----------------------
    "add_checklist": "tasks:add_checklist",
    "list_checklist": "tasks:list_checklist",

    # -----------------------
    # Delegation
    # -----------------------
    "add_delegation": "tasks:add_delegation",
    "list_delegation": "tasks:list_delegation",

    # -----------------------
    # Tickets
    # -----------------------
    "add_ticket": "tasks:add_help_ticket",
    "list_all_tickets": "tasks:list_help_ticket",
    "assigned_to_me": "tasks:assigned_to_me",
    "assigned_by_me": "tasks:assigned_by_me",

    # -----------------------
    # Petty Cash
    # -----------------------
    "petty_cash_list": "petty_cash:list_requests",
    "petty_cash_apply": "petty_cash:apply_request",

    # -----------------------
    # Sales
    # -----------------------
    "add_sales_plan": "sales:sales_plan_add",
    "list_sales_plan": "sales:sales_plan_list",

    # -----------------------
    # Reimbursement
    # -----------------------
    # Employee
    "reimbursement_apply": "reimbursement:my_reimbursements",
    "reimbursement_list": "reimbursement:my_reimbursements",
    # Manager
    "reimbursement_manager_pending": "reimbursement:manager_queue",
    "reimbursement_manager_review": "reimbursement:manager_queue",
    # Management
    "reimbursement_management_pending": "reimbursement:management_queue",
    "reimbursement_management_review": "reimbursement:management_queue",
    # Finance
    "reimbursement_finance_pending": "reimbursement:finance_queue",
    "reimbursement_finance_review": "reimbursement:finance_queue",
    # Admin console (Bills Summary as primary landing)
    "reimbursement_admin": "reimbursement:admin_bills_summary",

    # -----------------------
    # Reports
    # -----------------------
    "doer_tasks": "reports:doer_tasks",
    "weekly_mis_score": "reports:weekly_mis_score",
    "performance_score": "reports:performance_score",

    # -----------------------
    # Users / Settings
    # -----------------------
    "list_users": "users:list_users",
    "add_user": "users:add_user",
    "system_settings": "settings:system_settings",
    "authorized_numbers": "settings:authorized_list",

    # -----------------------
    # Clients (optional area; map to your actual URL names if present)
    # -----------------------
    "manage_clients_add": "clients:add",
    "manage_clients_list": "clients:list",
    "manage_clients_edit": "clients:edit",
    "manage_clients_delete": "clients:delete",
    "manage_clients_upload": "clients:upload",
    "manage_clients_upload_dndrnd": "clients:upload_dndrnd",

    # -----------------------
    # Customer Group (optional; adjust if present)
    # -----------------------
    "customer_group_add": "customer_group:add",
    "customer_group_list": "customer_group:list",
    "customer_group_edit": "customer_group:edit",
    "customer_group_delete": "customer_group:delete",
    "customer_group_csv": "customer_group:csv",

    # -----------------------
    # WhatsApp Template (optional; adjust if present)
    # -----------------------
    "wa_template_add": "wa_template:add",
    "wa_template_list": "wa_template:list",
    "wa_template_edit": "wa_template:edit",
    "wa_template_delete": "wa_template:delete",

    # -----------------------
    # Master Tasks (optional; adjust if present)
    # -----------------------
    "mt_add_checklist": "master_tasks:add_checklist",
    "mt_list_checklist": "master_tasks:list_checklist",
    "mt_edit_checklist": "master_tasks:edit_checklist",
    "mt_delete_checklist": "master_tasks:delete_checklist",
    "mt_add_delegation": "master_tasks:add_delegation",
    "mt_list_delegation": "master_tasks:list_delegation",
    "mt_edit_delegation": "master_tasks:edit_delegation",
    "mt_delete_delegation": "master_tasks:delete_delegation",
    "mt_bulk_upload": "master_tasks:bulk_upload",
    "mt_delegation_planned_date_edit": "master_tasks:delegation_planned_date_edit",
    "mt_delegation_planned_date_list": "master_tasks:delegation_planned_date_list",

    # -----------------------
    # Organization (optional; adjust if present)
    # -----------------------
    "org_add_branch": "org:add_branch",
    "org_list_branch": "org:list_branch",
    "org_edit_branch": "org:edit_branch",
    "org_delete_branch": "org:delete_branch",
    "org_add_company": "org:add_company",
    "org_list_company": "org:list_company",
    "org_edit_company": "org:edit_company",
    "org_delete_company": "org:delete_company",
    "org_add_department": "org:add_department",
    "org_list_department": "org:list_department",
    "org_edit_department": "org:edit_department",
    "org_delete_department": "org:delete_department",
}
