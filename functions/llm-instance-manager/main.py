"""
LLMインスタンス管理Cloud Functions
Phase 1: 手動起動・停止機能
"""

import functions_framework
from google.cloud import compute_v1
import logging
import time
import requests
from datetime import datetime

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 設定
PROJECT_ID = "seo-optimize-464208"
ZONE = "asia-northeast1-c"
INSTANCE_NAME = "llm-gpu-instance"
HEALTH_CHECK_TIMEOUT = 300  # 5分

@functions_framework.http
def start_llm_instance(request):
    """LLMインスタンスを起動（スナップショットから復元が必要な場合は自動復元）"""
    try:
        logger.info(f"Starting LLM instance: {INSTANCE_NAME}")

        # Compute Engine クライアント初期化
        instances_client = compute_v1.InstancesClient()

        try:
            # インスタンス状態確認
            instance = instances_client.get(
                project=PROJECT_ID,
                zone=ZONE,
                instance=INSTANCE_NAME
            )

            current_status = instance.status
            logger.info(f"Current instance status: {current_status}")

            if current_status == "RUNNING":
                return {
                    "status": "success",
                    "message": "Instance is already running",
                    "instance_name": INSTANCE_NAME,
                    "external_ip": get_external_ip(instance),
                    "timestamp": datetime.now().isoformat()
                }
            elif current_status in ["STOPPED", "TERMINATED"]:
                logger.info("Instance exists but is stopped, starting it...")

        except Exception as e:
            if "was not found" in str(e):
                logger.info("Instance not found, creating from latest snapshot...")
                # スナップショットから新しいインスタンスを作成（非同期）
                return create_instance_from_snapshot_async()
            else:
                raise e

        # インスタンス起動
        operation = instances_client.start(
            project=PROJECT_ID,
            zone=ZONE,
            instance=INSTANCE_NAME
        )

        logger.info(f"Start operation initiated: {operation.name}")

        # 起動操作を開始したら即座にレスポンス返却（非同期処理）
        logger.info(f"Start operation initiated: {operation.name}")

        response = {
            "status": "success",
            "message": "Instance startup initiated. Check status with get-llm-status API.",
            "instance_name": INSTANCE_NAME,
            "operation_name": operation.name,
            "estimated_time": "2-5 minutes",
            "timestamp": datetime.now().isoformat()
        }

        logger.info(f"Instance started: {response}")
        return response

    except Exception as e:
        error_message = f"Failed to start instance: {str(e)}"
        logger.error(error_message)
        return {"status": "error", "message": error_message}

@functions_framework.http
def stop_llm_instance(request):
    """LLMインスタンスを停止・削除（コスト最適化のため）"""
    try:
        logger.info(f"Stopping and deleting LLM instance: {INSTANCE_NAME}")

        # Compute Engine クライアント初期化
        instances_client = compute_v1.InstancesClient()

        try:
            # インスタンス状態確認
            instance = instances_client.get(
                project=PROJECT_ID,
                zone=ZONE,
                instance=INSTANCE_NAME
            )

            current_status = instance.status
            logger.info(f"Current instance status: {current_status}")

            if current_status in ["TERMINATED", "STOPPED"]:
                # 停止済みインスタンスを削除
                logger.info("Instance is stopped, deleting it...")
                operation = instances_client.delete(
                    project=PROJECT_ID,
                    zone=ZONE,
                    instance=INSTANCE_NAME
                )
                wait_for_operation(operation, "instance deletion")

                return {
                    "status": "success",
                    "message": "Stopped instance deleted successfully",
                    "instance_name": INSTANCE_NAME,
                    "timestamp": datetime.now().isoformat()
                }

            # 実行中インスタンスを停止してから削除
            logger.info("Stopping running instance...")
            stop_operation = instances_client.stop(
                project=PROJECT_ID,
                zone=ZONE,
                instance=INSTANCE_NAME
            )
            wait_for_operation(stop_operation, "instance stop")

            logger.info("Deleting stopped instance...")
            delete_operation = instances_client.delete(
                project=PROJECT_ID,
                zone=ZONE,
                instance=INSTANCE_NAME
            )
            wait_for_operation(delete_operation, "instance deletion")

            response = {
                "status": "success",
                "message": "Instance stopped and deleted successfully",
                "instance_name": INSTANCE_NAME,
                "timestamp": datetime.now().isoformat()
            }

            logger.info(f"Instance stopped and deleted: {response}")
            return response

        except Exception as e:
            if "was not found" in str(e):
                return {
                    "status": "success",
                    "message": "Instance is already deleted",
                    "instance_name": INSTANCE_NAME,
                    "timestamp": datetime.now().isoformat()
                }
            else:
                raise e

    except Exception as e:
        error_message = f"Failed to stop/delete instance: {str(e)}"
        logger.error(error_message)
        return {"status": "error", "message": error_message}

@functions_framework.http
def get_llm_status(request):
    """LLMインスタンスの状態を取得"""
    try:
        logger.info(f"Getting LLM instance status: {INSTANCE_NAME}")

        # Compute Engine クライアント初期化
        instances_client = compute_v1.InstancesClient()

        # インスタンス情報取得
        instance = instances_client.get(
            project=PROJECT_ID,
            zone=ZONE,
            instance=INSTANCE_NAME
        )

        external_ip = get_external_ip(instance)

        # APIヘルスチェック（インスタンスが実行中の場合）
        api_status = None
        if instance.status == "RUNNING" and external_ip:
            api_status = check_api_health(external_ip)

        response = {
            "status": "success",
            "instance_status": instance.status,
            "instance_name": INSTANCE_NAME,
            "external_ip": external_ip,
            "api_status": api_status,
            "timestamp": datetime.now().isoformat()
        }

        logger.info(f"Instance status: {response}")
        return response

    except Exception as e:
        error_message = f"Failed to get instance status: {str(e)}"
        logger.error(error_message)
        return {"status": "error", "message": error_message}

def get_external_ip(instance):
    """インスタンスの外部IPを取得"""
    try:
        if instance.network_interfaces:
            access_configs = instance.network_interfaces[0].access_configs
            if access_configs:
                return access_configs[0].nat_ip
    except Exception as e:
        logger.warning(f"Failed to get external IP: {str(e)}")
    return None

def check_api_health(external_ip):
    """API ヘルスチェック"""
    try:
        health_url = f"http://{external_ip}:8080/health"
        response = requests.get(health_url, timeout=10)

        if response.status_code == 200:
            return {
                "status": "healthy",
                "response_time": response.elapsed.total_seconds(),
                "data": response.json()
            }
        else:
            return {
                "status": "unhealthy",
                "status_code": response.status_code
            }
    except Exception as e:
        return {
            "status": "unreachable",
            "error": str(e)
        }

def wait_for_health_check(external_ip, timeout=HEALTH_CHECK_TIMEOUT):
    """ヘルスチェック完了待機"""
    if not external_ip:
        return {"status": "no_ip", "message": "External IP not available"}

    start_time = time.time()
    while time.time() - start_time < timeout:
        api_status = check_api_health(external_ip)

        if api_status["status"] == "healthy":
            return {
                "status": "healthy",
                "wait_time": round(time.time() - start_time, 2),
                "api_response": api_status
            }

        logger.info(f"Waiting for API to be healthy... ({api_status['status']})")
        time.sleep(15)

    return {
        "status": "timeout",
        "wait_time": timeout,
        "message": "Health check timeout"
    }

def create_instance_from_snapshot_async():
    """最新のスナップショットからインスタンスを作成（非同期対応版）"""
    try:
        logger.info("Creating instance from latest snapshot (async mode)...")

        # スナップショット一覧取得
        snapshots_client = compute_v1.SnapshotsClient()
        snapshots = snapshots_client.list(project=PROJECT_ID)

        # LLM関連のスナップショットをフィルタリングして最新を取得
        llm_snapshots = [s for s in snapshots if s.name.startswith("llm-disk")]
        if not llm_snapshots:
            return {"status": "error", "message": "No LLM snapshots found"}

        # 最新のスナップショットを選択（名前でソート）
        latest_snapshot = sorted(llm_snapshots, key=lambda s: s.creation_timestamp)[-1]
        logger.info(f"Using latest snapshot: {latest_snapshot.name}")

        # ディスク作成操作を開始（非同期）
        disks_client = compute_v1.DisksClient()
        disk_body = {
            "name": INSTANCE_NAME,
            "source_snapshot": latest_snapshot.self_link,
            "type": f"projects/{PROJECT_ID}/zones/{ZONE}/diskTypes/pd-balanced"
        }

        # 既存ディスクがある場合は削除
        try:
            existing_disk = disks_client.get(project=PROJECT_ID, zone=ZONE, disk=INSTANCE_NAME)
            logger.info(f"Deleting existing disk: {INSTANCE_NAME}")
            delete_op = disks_client.delete(project=PROJECT_ID, zone=ZONE, disk=INSTANCE_NAME)
            # 削除完了は待機しない（非同期処理）
            logger.info(f"Disk deletion initiated: {delete_op.name}")
            time.sleep(10)  # 短時間だけ待機
        except Exception:
            logger.info("No existing disk to delete")

        # 新しいディスクを作成開始
        logger.info("Creating disk from snapshot (async)...")
        disk_operation = disks_client.insert(project=PROJECT_ID, zone=ZONE, disk_resource=disk_body)

        # ディスク作成の完了を待機（最大60秒）
        logger.info("Waiting for disk creation to complete (timeout: 60s)...")
        wait_for_operation_with_timeout(disk_operation, "disk creation", timeout=60)

        # インスタンス作成
        logger.info("Creating instance from restored disk...")
        instances_client = compute_v1.InstancesClient()
        instance_body = {
            "name": INSTANCE_NAME,
            "machine_type": f"projects/{PROJECT_ID}/zones/{ZONE}/machineTypes/n1-highmem-4",
            "disks": [{
                "boot": True,
                "auto_delete": True,
                "device_name": INSTANCE_NAME,
                "source": f"projects/{PROJECT_ID}/zones/{ZONE}/disks/{INSTANCE_NAME}"
            }],
            "network_interfaces": [{
                "network": "projects/seo-optimize-464208/global/networks/default",
                "access_configs": [{
                    "type": "ONE_TO_ONE_NAT",
                    "name": "External NAT"
                }]
            }],
            "guest_accelerators": [{
                "accelerator_type": f"projects/{PROJECT_ID}/zones/{ZONE}/acceleratorTypes/nvidia-tesla-t4",
                "accelerator_count": 1
            }],
            "scheduling": {
                "on_host_maintenance": "TERMINATE",
                "automatic_restart": False
            },
            "service_accounts": [{
                "email": "default",
                "scopes": [
                    "https://www.googleapis.com/auth/devstorage.read_only",
                    "https://www.googleapis.com/auth/logging.write",
                    "https://www.googleapis.com/auth/monitoring.write"
                ]
            }]
        }

        # インスタンス作成開始（非同期）
        logger.info("Starting instance creation...")
        instance_operation = instances_client.insert(
            project=PROJECT_ID,
            zone=ZONE,
            instance_resource=instance_body
        )

        logger.info(f"Instance creation initiated: operation {instance_operation.name}")

        return {
            "status": "success",
            "message": "Instance creation from snapshot initiated. Monitor with get-llm-status API.",
            "estimated_time": "3-7 minutes",
            "snapshot_name": latest_snapshot.name,
            "operation_name": instance_operation.name,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        error_message = f"Failed to initiate instance creation from snapshot: {str(e)}"
        logger.error(error_message)
        return {"status": "error", "message": error_message}

def create_instance_from_snapshot():
    """最新のスナップショットからインスタンスを作成（元の同期版）"""
    try:
        logger.info("Creating instance from latest snapshot...")

        # スナップショット一覧取得
        snapshots_client = compute_v1.SnapshotsClient()
        snapshots = snapshots_client.list(project=PROJECT_ID)

        # LLM関連のスナップショットをフィルタリングして最新を取得
        llm_snapshots = [s for s in snapshots if s.name.startswith("llm-disk")]
        if not llm_snapshots:
            return {"status": "error", "message": "No LLM snapshots found"}

        # 最新のスナップショットを選択（名前でソート）
        latest_snapshot = sorted(llm_snapshots, key=lambda s: s.creation_timestamp)[-1]
        logger.info(f"Using latest snapshot: {latest_snapshot.name}")

        # ディスク作成
        disks_client = compute_v1.DisksClient()
        disk_body = {
            "name": INSTANCE_NAME,
            "source_snapshot": latest_snapshot.self_link,
            "type": f"projects/{PROJECT_ID}/zones/{ZONE}/diskTypes/pd-balanced"
        }

        # 既存ディスクがある場合は削除
        try:
            existing_disk = disks_client.get(project=PROJECT_ID, zone=ZONE, disk=INSTANCE_NAME)
            logger.info(f"Deleting existing disk: {INSTANCE_NAME}")
            delete_op = disks_client.delete(project=PROJECT_ID, zone=ZONE, disk=INSTANCE_NAME)
            wait_for_operation(delete_op, "disk deletion")
        except Exception:
            logger.info("No existing disk to delete")

        # 新しいディスクを作成
        logger.info("Creating disk from snapshot...")
        disk_operation = disks_client.insert(project=PROJECT_ID, zone=ZONE, disk_resource=disk_body)
        wait_for_operation(disk_operation, "disk creation")

        # インスタンス作成
        instances_client = compute_v1.InstancesClient()
        instance_body = {
            "name": INSTANCE_NAME,
            "machine_type": f"projects/{PROJECT_ID}/zones/{ZONE}/machineTypes/n1-highmem-4",
            "disks": [{
                "boot": True,
                "auto_delete": True,
                "device_name": INSTANCE_NAME,
                "source": f"projects/{PROJECT_ID}/zones/{ZONE}/disks/{INSTANCE_NAME}"
            }],
            "network_interfaces": [{
                "network": "projects/seo-optimize-464208/global/networks/default",
                "access_configs": [{
                    "type": "ONE_TO_ONE_NAT",
                    "name": "External NAT"
                }]
            }],
            "guest_accelerators": [{
                "accelerator_type": f"projects/{PROJECT_ID}/zones/{ZONE}/acceleratorTypes/nvidia-tesla-t4",
                "accelerator_count": 1
            }],
            "scheduling": {
                "on_host_maintenance": "TERMINATE",
                "automatic_restart": False
            },
            "service_accounts": [{
                "email": "default",
                "scopes": [
                    "https://www.googleapis.com/auth/devstorage.read_only",
                    "https://www.googleapis.com/auth/logging.write",
                    "https://www.googleapis.com/auth/monitoring.write"
                ]
            }]
        }

        logger.info("Creating instance...")
        instance_operation = instances_client.insert(
            project=PROJECT_ID,
            zone=ZONE,
            instance_resource=instance_body
        )
        wait_for_operation(instance_operation, "instance creation")

        logger.info("Instance created successfully from snapshot")
        return {"status": "success", "message": "Instance created from snapshot"}

    except Exception as e:
        error_message = f"Failed to create instance from snapshot: {str(e)}"
        logger.error(error_message)
        return {"status": "error", "message": error_message}

def wait_for_operation(operation, operation_name):
    """オペレーション完了待機"""
    operation_client = compute_v1.ZoneOperationsClient()

    while True:
        result = operation_client.get(
            project=PROJECT_ID,
            zone=ZONE,
            operation=operation.name
        )

        if result.status == "DONE":
            if hasattr(result, 'error') and result.error:
                error_message = f"{operation_name} failed: {result.error}"
                logger.error(error_message)
                raise Exception(error_message)
            logger.info(f"{operation_name} completed successfully")
            break

        logger.info(f"Waiting for {operation_name} to complete...")
        time.sleep(5)

def wait_for_operation_with_timeout(operation, operation_name, timeout=60):
    """オペレーション完了待機（タイムアウト付き）"""
    operation_client = compute_v1.ZoneOperationsClient()
    start_time = time.time()

    while time.time() - start_time < timeout:
        result = operation_client.get(
            project=PROJECT_ID,
            zone=ZONE,
            operation=operation.name
        )

        if result.status == "DONE":
            if hasattr(result, 'error') and result.error:
                error_message = f"{operation_name} failed: {result.error}"
                logger.error(error_message)
                raise Exception(error_message)
            logger.info(f"{operation_name} completed successfully")
            return

        logger.info(f"Waiting for {operation_name} to complete... ({int(time.time() - start_time)}s)")
        time.sleep(3)

    # タイムアウトでも処理を継続（ログで警告）
    logger.warning(f"{operation_name} timeout after {timeout}s, continuing with instance creation...")