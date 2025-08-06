import pandas as pd
import json
import logging

# Ensure these imports are correct and match your file names
from powerbi_api_utils import load_config, get_api_constants, get_access_token, get_paginated_data, get_paginated_admin_groups
from fabric_utils import save_to_fabric_warehouse # get_or_create_spark_session is removed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_all_metadata(token, base_url, admin_base_url, verify_ssl, include_report_app_users=True):
    """Orchestrates the fetching of all Power BI metadata."""
    print("\n--- Fetching Tenant-Level Admin Data ---")
    print("\nFetching Capacities...")
    capacities_url = f"{admin_base_url}/capacities"
    capacities_data = get_paginated_data(token, capacities_url, verify_ssl)
    capacities_df = pd.DataFrame(capacities_data)
    if not capacities_df.empty:
        capacities_df['admins'] = capacities_df['admins'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else (x if x is not None else None)
        )
        capacities_df.columns = [col.replace('.', '_').replace(' ', '_') for col in capacities_df.columns]
    print("\n--- Fetching Gateways (User/Shared Scope) and their Data Sources and Users ---")
    gateways_url = f"{base_url}/gateways"
    gateways_data = get_paginated_data(token=token, url=gateways_url, verify_ssl=verify_ssl)
    processed_gateways = []
    all_gateway_datasources = []
    all_datasource_users = []
    if gateways_data:
        for gateway in gateways_data:
            gateway_id = gateway.get('id')
            filtered_gateway = {k: v for k, v in gateway.items() if k not in ['publicKey', 'gatewayAnnotation']}
            processed_gateways.append(filtered_gateway)
            if gateway_id:
                print(f"  - Fetching datasources for Gateway ID: {gateway_id}")
                datasources_url = f"{base_url}/gateways/{gateway_id}/datasources"
                datasources_data = get_paginated_data(token=token, url=datasources_url, verify_ssl=verify_ssl)
                for ds in datasources_data:
                    datasource_id = ds.get('id')
                    ds['gatewayId'] = gateway_id
                    if 'credentialDetails' in ds and isinstance(ds['credentialDetails'], dict):
                        credential_details = ds.pop('credentialDetails')
                        for k, v in credential_details.items():
                            ds[f'credentialDetails_{k}'] = v
                    all_gateway_datasources.append(ds)
                    if datasource_id:
                        print(f"    - Fetching users for Datasource ID: {datasource_id} (Gateway: {gateway_id})")
                        datasource_users_url = f"{base_url}/gateways/{gateway_id}/datasources/{datasource_id}/users"
                        users_data = get_paginated_data(token=token, url=datasource_users_url, verify_ssl=verify_ssl)
                        for user in users_data:
                            user['gatewayId'] = gateway_id
                            user['datasourceId'] = datasource_id
                            all_datasource_users.append(user)
    gateways_df = pd.DataFrame(processed_gateways)
    if not gateways_df.empty:
        gateways_df.columns = [col.replace('.', '_').replace(' ', '_') for col in gateways_df.columns]
    gateway_datasources_df = pd.DataFrame(all_gateway_datasources)
    if not gateway_datasources_df.empty:
        gateway_datasources_df.columns = [col.replace('.', '_').replace(' ', '_') for col in gateway_datasources_df.columns]
        if 'details' in gateway_datasources_df.columns:
            details_df = pd.json_normalize(gateway_datasources_df['details'])
            details_df.columns = [f"details_{col.replace('.', '_').replace(' ', '_')}" for col in details_df.columns]
            gateway_datasources_df = pd.concat([gateway_datasources_df.drop(columns=['details']), details_df], axis=1)
    gateway_datasource_users_df = pd.DataFrame(all_datasource_users)
    if not gateway_datasource_users_df.empty:
        gateway_datasource_users_df.columns = [col.replace('.', '_').replace(' ', '_') for col in gateway_datasource_users_df.columns]
    print("\n--- Fetching Workspace-Level Data ---")
    workspaces_base_url = f"{admin_base_url}/groups"
    expand_payload = "users,reports,datasets,dataflows"
    workspace_filter = "type eq 'Workspace' and state eq 'Active'"
    workspaces_data = get_paginated_admin_groups(
        token,
        workspaces_base_url,
        expand_payload,
        verify_ssl,
        filter_str=workspace_filter
    )
    all_workspaces, all_workspace_users, all_reports, all_datasets, all_dataflows = [], [], [], [], []
    for ws in workspaces_data:
        ws_id = ws['id']
        ws_name = ws['name']
        print(f"\nProcessing Workspace: '{ws_name}' ({ws_id})")
        all_workspaces.append({k: ws.get(k) for k in ['id', 'name', 'isOnDedicatedCapacity', 'capacityId', 'type', 'state']})
        for user in ws.get('users', []):
            user.update({'workspace_id': ws_id, 'workspace_name': ws_name})
            all_workspace_users.append(user)
        for report in ws.get('reports', []):
            report.update({'workspace_id': ws_id, 'workspace_name': ws_name})
            all_reports.append(report)
        for dataset in ws.get('datasets', []):
            dataset.update({'workspace_id': ws_id, 'workspace_name': ws_name})
            all_datasets.append(dataset)
        for dataflow in ws.get('dataflows', []):
            dataflow.update({'workspace_id': ws_id, 'workspace_name': ws_name})
            all_dataflows.append(dataflow)
    workspaces_df = pd.DataFrame(all_workspaces)
    if not workspaces_df.empty:
        workspaces_df.columns = [col.replace('.', '_').replace(' ', '_') for col in workspaces_df.columns]
    workspace_users_df = pd.DataFrame(all_workspace_users)
    if not workspace_users_df.empty:
        workspace_users_df.columns = [col.replace('.', '_').replace(' ', '_') for col in workspace_users_df.columns]
        if 'profile' in workspace_users_df.columns:
            profile_df = pd.json_normalize(workspace_users_df['profile']).add_prefix('profile_')
            profile_df.columns = [col.replace('.', '_').replace(' ', '_') for col in profile_df.columns]
            workspace_users_df = pd.concat([workspace_users_df.drop(columns=['profile']), profile_df], axis=1)
    reports_df = pd.DataFrame(all_reports)
    if not reports_df.empty:
        reports_df.columns = [col.replace('.', '_').replace(' ', '_') for col in reports_df.columns]
    datasets_df = pd.DataFrame(all_datasets)
    if not datasets_df.empty:
        datasets_df.columns = [col.replace('.', '_').replace(' ', '_') for col in datasets_df.columns]
        for col in ['qnaQuestions', 'queryMetrics']:
            if col in datasets_df.columns:
                datasets_df[col] = datasets_df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
    dataflows_df = pd.DataFrame(all_dataflows)
    if not dataflows_df.empty:
        dataflows_df.columns = [col.replace('.', '_').replace(' ', '_') for col in dataflows_df.columns]
    all_apps = []
    all_app_users = []
    if include_report_app_users:
        print("\n--- Fetching Apps and App Users ---")
        apps_url = f"{admin_base_url}/apps"
        apps_data = get_paginated_data(token, apps_url, verify_ssl, params={'$top': 5000})
        for app in apps_data:
            app_id = app['id']
            app_name = app['name']
            print(f"    - Processing App: '{app_name}' ({app_id})")
            all_apps.append(app)
            print(f"        - Fetching users for App ID: {app_id}")
            app_users_url = f"{admin_base_url}/apps/{app_id}/users"
            app_users_data = get_paginated_data(token, app_users_url, verify_ssl)
            for user in app_users_data:
                user.update({'app_id': app_id, 'app_name': app_name})
                all_app_users.append(user)
    else:
        print("\n--- Skipping Apps and App Users fetch as per user's choice ---")
    apps_df = pd.DataFrame(all_apps)
    if not apps_df.empty:
        apps_df.columns = [col.replace('.', '_').replace(' ', '_') for col in apps_df.columns]
        if 'lastUpdateDateTime' in apps_df.columns:
            apps_df['lastUpdateDateTime'] = pd.to_datetime(apps_df['lastUpdateDateTime'], errors='coerce')
    app_users_df = pd.DataFrame(all_app_users)
    if not app_users_df.empty:
        app_users_df.columns = [col.replace('.', '_').replace(' ', '_') for col in app_users_df.columns]
        if 'profile' in app_users_df.columns:
            profile_df = pd.json_normalize(app_users_df['profile']).add_prefix('profile_')
            profile_df.columns = [col.replace('.', '_').replace(' ', '_') for col in profile_df.columns]
            app_users_df = pd.concat([app_users_df.drop(columns=['profile']), profile_df], axis=1)
    all_report_users = []
    if include_report_app_users:
        print("\n--- Fetching Report-Level User Access (this may take a long time) ---")
        if not reports_df.empty:
            for index, report in reports_df.iterrows():
                report_id = report['id']
                report_name = report['name']
                ws_id = report['workspace_id']
                print(f"    - Fetching users for Report: '{report_name}' in Workspace ID: {ws_id}")
                report_users_url = f"{admin_base_url}/groups/{ws_id}/reports/{report_id}/users"
                report_users_data = get_paginated_data(token, report_users_url, verify_ssl)
                for user in report_users_data:
                    user.update({'report_id': report_id, 'report_name': report_name, 'workspace_id': ws_id})
                    all_report_users.append(user)
    else:
        print("\n--- Skipping Report-Level User Access fetch as per user's choice ---")
    report_users_df = pd.DataFrame(all_report_users)
    if not report_users_df.empty:
        report_users_df.columns = [col.replace('.', '_').replace(' ', '_') for col in report_users_df.columns]
        if 'profile' in report_users_df.columns:
            profile_df = pd.json_normalize(report_users_df['profile']).add_prefix('profile_')
            profile_df.columns = [col.replace('.', '_').replace(' ', '_') for col in profile_df.columns]
            report_users_df = pd.concat([report_users_df.drop(columns=['profile']), profile_df], axis=1)
    return {
        "capacities": capacities_df,
        "gateways": gateways_df,
        "gateway_datasources": gateway_datasources_df,
        "gateway_datasource_users": gateway_datasource_users_df,
        "workspaces": workspaces_df,
        "workspace_users": workspace_users_df,
        "reports": reports_df,
        "report_users": report_users_df,
        "datasets": datasets_df,
        "dataflows": dataflows_df,
        "apps": apps_df,
        "app_users": app_users_df,
    }


def run_extraction_to_fabric():
    """
    Main function to run the metadata extraction and save to Fabric Warehouse process.
    Designed for execution within a Microsoft Fabric notebook.
    """
    try:
        # Get Spark session directly from the global scope (where print(spark) works)
        print("1. Attempting to get SparkSession directly from notebook globals...")
        # Accessing globals() from the function defined in the main notebook cell
        # should correctly find the 'spark' object.
        spark_session = globals()['spark']
        print("2. SparkSession successfully obtained.")

        # Load configuration (assumes config.json is accessible in the notebook environment)
        print("3. Loading configuration...")
        config = load_config()
        AUTHORITY, SCOPE, BASE_URL, ADMIN_BASE_URL = get_api_constants(config["TENANT_ID"])
        print("4. Configuration loaded.")

        print("5. Attempting to authenticate Power BI with Service Principal...")
        access_token = get_access_token(
            config["CLIENT_ID"],
            config["CLIENT_SECRET"],
            AUTHORITY,
            SCOPE
        )
        print("6. Successfully authenticated and acquired access token.")

        while True:
            user_input = input("Do you want to pull in Report and App users? (yes/no): ").lower().strip()
            if user_input in ['yes', 'y']:
                include_users = True
                break
            elif user_input in ['no', 'n']:
                include_users = False
                break
            else:
                print("Invalid input. Please enter 'yes' or 'no'.")

        print("7. Starting metadata extraction...")
        all_data_pandas_dfs = get_all_metadata(
            access_token,
            BASE_URL,
            ADMIN_BASE_URL,
            config["VERIFY_SSL"],
            include_report_app_users=include_users
        )
        
        print("\n8. Saving extracted metadata to Microsoft Fabric Warehouse...")
        # Pass the obtained spark_session directly to save_to_fabric_warehouse
        save_to_fabric_warehouse(all_data_pandas_dfs, warehouse_schema="powerbi_metadata", spark_session=spark_session)
        print("9. Metadata extraction and saving to Fabric Warehouse completed.")

    except Exception as e:
        # Now, the 'raise' will ensure you get the full traceback if any other error occurs
        print(f"\nAn error occurred during the process: {e}")
        print("Please check your configuration, permissions, and ensure you are running this in a Fabric notebook with necessary libraries.")
        raise # Re-raise the exception to get the full traceback for debugging