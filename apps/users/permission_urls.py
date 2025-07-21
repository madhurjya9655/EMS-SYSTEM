PERMISSION_URLS = {
    # Leave
    'leave_apply': 'leave:apply_leave',
    'leave_list': 'leave:my_leaves',
    'leave_pending_manager': 'leave:pending_leaves',
    'leave_pending_hr': 'leave:hr_leaves',
    # Checklist
    'add_checklist': 'tasks:add_checklist',
    'list_checklist': 'tasks:list_checklist',
    # Delegation
    'add_delegation': 'tasks:add_delegation',
    'list_delegation': 'tasks:list_delegation',
    # Tickets
    'add_ticket': 'tasks:add_help_ticket',
    'list_all_tickets': 'tasks:list_help_ticket',
    'assigned_to_me': 'tasks:assigned_to_me',
    'assigned_by_me': 'tasks:assigned_by_me',
    # Petty Cash
    'petty_cash_list': 'petty_cash:list_requests',
    'petty_cash_apply': 'petty_cash:apply_request',
    # Sales
    'add_sales_plan': 'sales:sales_plan_add',
    'list_sales_plan': 'sales:sales_plan_list',
    # Reimbursement
    'reimbursement_apply': 'reimbursement:my_reimbursements',
    'reimbursement_list': 'reimbursement:my_reimbursements',
    # Reports
    'doer_tasks': 'reports:doer_tasks',
    'weekly_mis_score': 'reports:weekly_mis_score',
    'performance_score': 'reports:performance_score',
    # Users
    'list_users': 'users:list_users',
    'add_user': 'users:add_user',
    'system_settings': 'settings:system_settings',
    'authorized_numbers': 'settings:authorized_list',
    # Add more as needed
}
