"""
インスタンス管理 Cloud Function
ローカルLLMインスタンスの開始・停止・復元を処理
"""
import functions_framework
import json
import time
from google.cloud import compute_v1
import logging

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 設定
PROJECT_ID = 'seo-optimize-464208'
ZONE = 'asia-northeast1-c'
DEFAULT_INSTANCE_NAME = 'llm-gpu-instance'
SNAPSHOT_NAME = 'llm-disk-20251203-153422'

def get_instance_status(instance_name):
    """インスタンスの状態を取得"""
    try:
        client = compute_v1.InstancesClient()
        instance = client.get(project=PROJECT_ID, zone=ZONE, instance=instance_name)
        return instance.status, instance.id
    except Exception as e:
        if "not found" in str(e).lower():
            return "NOT_FOUND", None
        logger.error(f"Failed to get instance status: {str(e)}")
        return "ERROR", None

def start_instance(instance_name):
    """インスタンスを開始"""
    try:
        client = compute_v1.InstancesClient()
        operation = client.start(project=PROJECT_ID, zone=ZONE, instance=instance_name)

        # 操作の完了を待機
        wait_for_operation(operation.name)

        return True, f"Instance {instance_name} started successfully"
    except Exception as e:
        logger.error(f"Failed to start instance: {str(e)}")
        return False, str(e)

def stop_instance(instance_name):
    """インスタンスを停止"""
    try:
        client = compute_v1.InstancesClient()
        operation = client.stop(project=PROJECT_ID, zone=ZONE, instance=instance_name)

        # 操作の完了を待機
        wait_for_operation(operation.name)

        return True, f"Instance {instance_name} stopped successfully"
    except Exception as e:
        logger.error(f"Failed to stop instance: {str(e)}")
        return False, str(e)

def delete_instance(instance_name):
    """インスタンスを削除"""
    try:
        client = compute_v1.InstancesClient()
        operation = client.delete(project=PROJECT_ID, zone=ZONE, instance=instance_name)

        # 操作の完了を待機
        wait_for_operation(operation.name)

        return True, f"Instance {instance_name} deleted successfully"
    except Exception as e:
        logger.error(f"Failed to delete instance: {str(e)}")
        return False, str(e)

def restore_from_snapshot(instance_name, snapshot_name):
    """スナップショットからインスタンスを復元"""
    try:
        # 既存のインスタンスがある場合は削除
        status, _ = get_instance_status(instance_name)
        if status != "NOT_FOUND":
            logger.info(f"Deleting existing instance: {instance_name}")
            success, message = delete_instance(instance_name)
            if not success:
                return False, f"Failed to delete existing instance: {message}"

            # 削除完了まで待機
            time.sleep(10)

        # ディスクが存在する場合は削除
        disk_client = compute_v1.DisksClient()
        try:
            disk_client.delete(project=PROJECT_ID, zone=ZONE, disk=instance_name)
            time.sleep(5)  # ディスク削除の完了を待機
        except Exception:
            pass  # ディスクが存在しない場合はエラーを無視

        # スナップショットから新しいディスクを作成
        logger.info(f"Creating disk from snapshot: {snapshot_name}")
        disk_operation = disk_client.insert(
            project=PROJECT_ID,
            zone=ZONE,
            disk_resource=compute_v1.Disk(
                name=instance_name,
                source_snapshot=f"projects/{PROJECT_ID}/global/snapshots/{snapshot_name}",
                type_=f"projects/{PROJECT_ID}/zones/{ZONE}/diskTypes/pd-standard"
            )
        )
        wait_for_operation(disk_operation.name)

        # 新しいインスタンスを作成
        logger.info(f"Creating instance: {instance_name}")
        instance_client = compute_v1.InstancesClient()

        # ネットワークインターフェースの設定
        network_interface = compute_v1.NetworkInterface(
            network="projects/{}/global/networks/default".format(PROJECT_ID),
            access_configs=[
                compute_v1.AccessConfig(
                    name="External NAT",
                    type_="ONE_TO_ONE_NAT",
                    network_tier="PREMIUM"
                )
            ]
        )

        # ディスクの設定
        attached_disk = compute_v1.AttachedDisk(
            boot=True,
            auto_delete=True,
            device_name=instance_name,
            source=f"projects/{PROJECT_ID}/zones/{ZONE}/disks/{instance_name}"
        )

        # インスタンスリソースの作成
        instance_resource = compute_v1.Instance(
            name=instance_name,
            machine_type=f"projects/{PROJECT_ID}/zones/{ZONE}/machineTypes/n1-standard-2",
            disks=[attached_disk],
            network_interfaces=[network_interface],
            tags=compute_v1.Tags(items=["llm-server"]),
            metadata=compute_v1.Metadata(
                items=[
                    compute_v1.Items(key="enable-oslogin", value="true")
                ]
            ),
            scheduling=compute_v1.Scheduling(
                preemptible=False
            ),
            shielded_instance_config=compute_v1.ShieldedInstanceConfig(
                enable_secure_boot=False,
                enable_vtpm=True,
                enable_integrity_monitoring=True
            )
        )

        instance_operation = instance_client.insert(
            project=PROJECT_ID,
            zone=ZONE,
            instance_resource=instance_resource
        )
        wait_for_operation(instance_operation.name)

        return True, f"Instance {instance_name} restored from snapshot {snapshot_name} successfully"

    except Exception as e:
        logger.error(f"Failed to restore from snapshot: {str(e)}")
        return False, str(e)

def wait_for_operation(operation_name):
    """操作の完了を待機"""
    zone_operations_client = compute_v1.ZoneOperationsClient()

    max_wait_time = 300  # 5分
    start_time = time.time()

    while time.time() - start_time < max_wait_time:
        operation = zone_operations_client.get(
            project=PROJECT_ID, zone=ZONE, operation=operation_name
        )

        if operation.status == 'DONE':
            if operation.error:
                raise Exception(f"Operation failed: {operation.error}")
            return

        time.sleep(5)

    raise Exception("Operation timeout")

@functions_framework.http
def manage_instance(request):
    """インスタンス管理のメイン関数"""
    # CORS設定
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    headers = {'Access-Control-Allow-Origin': '*'}

    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return json.dumps({'success': False, 'error': 'Invalid request body'}), 400, headers

        action = request_json.get('action')
        instance_name = request_json.get('instance_name', DEFAULT_INSTANCE_NAME)

        if not action:
            return json.dumps({'success': False, 'error': 'Action is required'}), 400, headers

        logger.info(f"Processing action: {action} for instance: {instance_name}")

        # 現在の状態を確認
        current_status, instance_id = get_instance_status(instance_name)
        logger.info(f"Current instance status: {current_status}")

        if action == 'start':
            if current_status == 'RUNNING':
                return json.dumps({
                    'success': True,
                    'message': f'Instance {instance_name} is already running',
                    'status': current_status
                }), 200, headers
            elif current_status == 'NOT_FOUND':
                return json.dumps({
                    'success': False,
                    'error': f'Instance {instance_name} does not exist. Use restore-from-snapshot action.'
                }), 404, headers
            elif current_status == 'TERMINATED':
                success, message = start_instance(instance_name)
            else:
                return json.dumps({
                    'success': False,
                    'error': f'Cannot start instance in {current_status} state'
                }), 400, headers

        elif action == 'stop':
            if current_status == 'TERMINATED':
                return json.dumps({
                    'success': True,
                    'message': f'Instance {instance_name} is already stopped',
                    'status': current_status
                }), 200, headers
            elif current_status == 'NOT_FOUND':
                return json.dumps({
                    'success': False,
                    'error': f'Instance {instance_name} does not exist'
                }), 404, headers
            elif current_status == 'RUNNING':
                success, message = stop_instance(instance_name)
            else:
                return json.dumps({
                    'success': False,
                    'error': f'Cannot stop instance in {current_status} state'
                }), 400, headers

        elif action == 'restore-from-snapshot':
            snapshot_name = request_json.get('snapshot_name', SNAPSHOT_NAME)
            success, message = restore_from_snapshot(instance_name, snapshot_name)

        else:
            return json.dumps({
                'success': False,
                'error': f'Unknown action: {action}'
            }), 400, headers

        if success:
            # 操作後の状態を確認
            final_status, _ = get_instance_status(instance_name)
            return json.dumps({
                'success': True,
                'message': message,
                'action': action,
                'instance_name': instance_name,
                'final_status': final_status
            }), 200, headers
        else:
            return json.dumps({
                'success': False,
                'error': message,
                'action': action,
                'instance_name': instance_name
            }), 500, headers

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return json.dumps({
            'success': False,
            'error': f'Unexpected error: {str(e)}'
        }), 500, headers

@functions_framework.http
def get_instance_info(request):
    """インスタンス情報の取得"""
    # CORS設定
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    headers = {'Access-Control-Allow-Origin': '*'}

    try:
        instance_name = request.args.get('instance_name', DEFAULT_INSTANCE_NAME)
        status, instance_id = get_instance_status(instance_name)

        # インスタンスが存在する場合、詳細情報を取得
        instance_info = None
        if status != "NOT_FOUND" and status != "ERROR":
            try:
                client = compute_v1.InstancesClient()
                instance = client.get(project=PROJECT_ID, zone=ZONE, instance=instance_name)

                external_ip = None
                if instance.network_interfaces:
                    for interface in instance.network_interfaces:
                        if interface.access_configs:
                            for config in interface.access_configs:
                                if config.nat_i_p:
                                    external_ip = config.nat_i_p
                                    break

                instance_info = {
                    'name': instance.name,
                    'status': instance.status,
                    'machine_type': instance.machine_type.split('/')[-1] if instance.machine_type else None,
                    'external_ip': external_ip,
                    'creation_timestamp': instance.creation_timestamp,
                    'tags': list(instance.tags.items) if instance.tags and instance.tags.items else []
                }

            except Exception as e:
                logger.warning(f"Could not get instance details: {str(e)}")

        return json.dumps({
            'success': True,
            'instance_name': instance_name,
            'status': status,
            'instance_id': instance_id,
            'instance_info': instance_info
        }), 200, headers

    except Exception as e:
        logger.error(f"Error getting instance info: {str(e)}")
        return json.dumps({
            'success': False,
            'error': str(e)
        }), 500, headers