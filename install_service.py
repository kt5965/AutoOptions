#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Windows 서비스 설치 스크립트
사용법: python install_service.py install    (서비스 설치)
       python install_service.py start      (서비스 시작)
       python install_service.py stop       (서비스 정지)
       python install_service.py remove     (서비스 제거)

관리자 권한으로 실행해야 합니다.
"""

import sys
import os
import logging
from pathlib import Path

# Windows 서비스 관련 패키지
try:
    import win32serviceutil
    import win32service
    import servicemanager
except ImportError:
    print("ERROR: pywin32 패키지가 필요합니다.")
    print("설치: pip install pywin32")
    sys.exit(1)

# 로그 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# 프로젝트 루트 디렉토리
PROJECT_ROOT = Path(__file__).parent


class AutoOptionsService(win32serviceutil.ServiceFramework):
    """AutoOptions 스케줄러 Windows 서비스 클래스"""
    
    _svc_name_ = "AutoOptions"
    _svc_display_name_ = "Auto Trading Scheduler"
    _svc_description_ = "Stock Auto Trading Scheduler Service"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.is_alive = True
        self.scheduler = None

    def SvcStop(self):
        """서비스 정지"""
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        servicemanager.LogInfoMsg(f"{self._svc_display_name_} 서비스 정지 중...")
        self.is_alive = False
        if self.scheduler:
            self.scheduler.shutdown()

    def SvcDoRun(self):
        """서비스 실행"""
        servicemanager.LogInfoMsg(f"{self._svc_display_name_} 서비스 시작됨")
        
        try:
            # 현재 디렉토리를 프로젝트 루트로 설정
            os.chdir(PROJECT_ROOT)
            sys.path.insert(0, str(PROJECT_ROOT))
            
            # scheduler 실행
            from scheduler import run_scheduler
            run_scheduler()
            
        except Exception as e:
            servicemanager.LogErrorMsg(f"서비스 실행 중 오류: {str(e)}")
            raise

    def SvcPause(self):
        """서비스 일시 정지"""
        servicemanager.LogInfoMsg(f"{self._svc_display_name_} 서비스 일시 정지됨")


def install_service():
    """Windows 서비스 설치"""
    try:
        logger.info("=" * 50)
        logger.info("AutoOptions Windows 서비스 설치")
        logger.info("=" * 50)
        logger.info(f"프로젝트 경로: {PROJECT_ROOT}")
        logger.info(f"서비스명: {AutoOptionsService._svc_name_}")
        logger.info(f"표시명: {AutoOptionsService._svc_display_name_}")
        
        # 이미 설치되어 있으면 제거
        try:
            win32serviceutil.RemoveService(AutoOptionsService._svc_name_)
            logger.info("기존 서비스 제거 완료")
        except:
            pass
        
        # 서비스 설치
        win32serviceutil.InstallService(
            AutoOptionsService,
            AutoOptionsService._svc_name_,
            AutoOptionsService._svc_display_name_,
            startType=win32service.SERVICE_AUTO_START,
        )
        logger.info("✓ 서비스 설치 완료")
        
        # 서비스 시작
        win32serviceutil.StartService(AutoOptionsService._svc_name_)
        logger.info("✓ 서비스 시작 완료")
        logger.info("\n서비스가 성공적으로 등록되었습니다.")
        logger.info("Windows 서비스 관리에서 'AutoOptions' 확인 가능합니다.")
        
    except Exception as e:
        logger.error(f"✗ 서비스 설치 실패: {str(e)}")
        sys.exit(1)


def remove_service():
    """Windows 서비스 제거"""
    try:
        logger.info("=" * 50)
        logger.info("AutoOptions Windows 서비스 제거")
        logger.info("=" * 50)
        
        # 서비스 정지
        try:
            win32serviceutil.StopService(AutoOptionsService._svc_name_)
            logger.info("서비스 정지 완료")
        except:
            pass
        
        # 서비스 제거
        win32serviceutil.RemoveService(AutoOptionsService._svc_name_)
        logger.info("✓ 서비스 제거 완료")
        
    except Exception as e:
        logger.error(f"✗ 서비스 제거 실패: {str(e)}")
        sys.exit(1)


def main():
    """메인 함수"""
    # 관리자 권한 확인 (Windows)
    try:
        import ctypes
        is_admin = ctypes.windll.shell32.IsUserAnAdmin()
    except:
        is_admin = True
    
    if not is_admin:
        logger.error("ERROR: 관리자 권한으로 실행하세요.")
        sys.exit(1)
    
    # 명령어 처리
    if len(sys.argv) > 1:
        if sys.argv[1].lower() == 'install':
            install_service()
        elif sys.argv[1].lower() == 'remove':
            remove_service()
        elif sys.argv[1].lower() == 'start':
            try:
                win32serviceutil.StartService(AutoOptionsService._svc_name_)
                logger.info("✓ 서비스가 시작되었습니다.")
            except Exception as e:
                logger.error(f"✗ 서비스 시작 실패: {str(e)}")
                sys.exit(1)
        elif sys.argv[1].lower() == 'stop':
            try:
                win32serviceutil.StopService(AutoOptionsService._svc_name_)
                logger.info("✓ 서비스가 정지되었습니다.")
            except Exception as e:
                logger.error(f"✗ 서비스 정지 실패: {str(e)}")
                sys.exit(1)
        else:
            print(__doc__)
    else:
        print(__doc__)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ['install', 'remove', 'start', 'stop']:
        main()
    else:
        # 서비스 디버그 모드
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(AutoOptionsService)
        servicemanager.StartServiceCtrlDispatcher()
