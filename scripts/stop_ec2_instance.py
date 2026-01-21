import boto3
import sys
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)

def stop_instance(instance_id, region='ap-northeast-2'):
    """
    지정된 EC2 인스턴스를 종료합니다.
    """
    ec2 = boto3.client('ec2', region_name=region)
    
    try:
        logging.info(f"인스턴스 종료 시도 중: {instance_id}")
        # IAM 역할(EC2_S3_Access_Role)을 통해 권한을 확인합니다.
        ec2.stop_instances(InstanceIds=[instance_id])
        logging.info(f"성공적으로 종료 명령을 보냈습니다: {instance_id}")
    except Exception as e:
        logging.error(f"인스턴스 종료 실패: {e}")
        sys.exit(1)

if __name__ == "__main__":
    TARGET_INSTANCE_ID = 'i-0542f479703e0fc2c' 
    stop_instance(TARGET_INSTANCE_ID)