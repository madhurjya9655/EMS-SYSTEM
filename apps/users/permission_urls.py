# apps/users/permission_urls.py
# Central map from our app-level permission codes → URL names.
# Used by menus, quick-links, and middleware to resolve where a permission should point.

PERMISSION_URLS = {
    # -----------------------
    # Leave
    # -----------------------
    "leave_apply": "leave:apply_leave",
    "leave_list": "leave:my_leaves",
    # Sidebar uses leave:manager_pending
    "leave_pending_manager": "leave:manager_pending",   # was leave:pending_leaves
    "leave_pending_hr": "leave:hr_leaves",              # optional, if used
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
    # KAM (Key Accounts) — behaves like any other module
    # Mirrors apps.kam.urls names
    "kam_dashboard": "kam:dashboard",
    "kam_manager": "kam:manager",
    "kam_manager_kpis": "kam:manager_kpis",
    "kam_plan": "kam:plan",
    "kam_visits": "kam:visits",
    "kam_visit_approve": "kam:visit_approve",
    "kam_visit_reject": "kam:visit_reject",
    "kam_call_new": "kam:call_new",
    "kam_collection_new": "kam:collection_new",
    "kam_customers": "kam:customers",
    "kam_targets": "kam:targets",
    "kam_targets_lines": "kam:targets_lines",
    "kam_reports": "kam:reports",
    "kam_collections_plan": "kam:collections_plan",
    "kam_export_kpi_csv": "kam:export_kpi_csv",
    "kam_sync_now": "kam:sync_now",
    "kam_sync_trigger": "kam:sync_trigger",
    "kam_sync_step": "kam:sync_step",

    # -----------------------
    # Reimbursement
    # -----------------------
    # Employee
    "reimbursement_apply": "reimbursement:my_reimbursements",
    "reimbursement_list": "reimbursement:my_reimbursements",

    # Manager — sidebar/login uses reimbursement:manager_pending
    "reimbursement_manager_pending": "reimbursement:manager_pending",
    "reimbursement_manager_review": "reimbursement:manager_pending",

    # Management (only if you have these URLs defined)
    "reimbursement_management_pending": "reimbursement:management_pending",
    "reimbursement_management_review": "reimbursement:management_pending",

    # Finance — keep legacy alias
    "reimbursement_finance_pending": "reimbursement:finance_pending",
    "reimbursement_finance_review": "reimbursement:finance_pending",
    "reimbursement_review_finance": "reimbursement:finance_pending",

    # Admin console — your sidebar links to reimbursement:admin_requests
    "reimbursement_admin": "reimbursement:admin_requests",

    # Analytics
    "reimbursement_analytics": "reimbursement:analytics_dashboard",

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
    # Clients (optional)
    # -----------------------
    "manage_clients_add": "clients:add",
    "manage_clients_list": "clients:list",
    "manage_clients_edit": "clients:edit",
    "manage_clients_delete": "clients:delete",
    "manage_clients_upload": "clients:upload",
    "manage_clients_upload_dndrnd": "clients:upload_dndrnd",

    # -----------------------
    # Customer Group (optional)
    # -----------------------
    "customer_group_add": "customer_group:add",
    "customer_group_list": "customer_group:list",
    "customer_group_edit": "customer_group:edit",
    "customer_group_delete": "customer_group:delete",
    "customer_group_csv": "customer_group:csv",

    # -----------------------
    # WhatsApp Template (optional)
    # -----------------------
    "wa_template_add": "wa_template:add",
    "wa_template_list": "wa_template:list",
    "wa_template_edit": "wa_template:edit",
    "wa_template_delete": "wa_template:delete",

    # -----------------------
    # Master Tasks (optional)
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
    # Organization (optional)
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
