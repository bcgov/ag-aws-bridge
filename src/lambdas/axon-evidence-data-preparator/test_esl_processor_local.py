from datetime import datetime
import sys
import os
from pathlib import Path

# Add packages to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src/lambdas/axon-evidence-data-preparator'))

from esl_processor.processor import ESLProcessor

class MockLogger:
    """Mock logger for local testing"""
    def log(self, event, level, message, context_data=None):
        print(f"[{level}] {event}: {message}")
        if context_data:
            print(f"  → {context_data}")

class MockDBManager:
    """Mock database manager for local testing"""
    def __init__(self, job_id):
        self.job_id = job_id
    
    def get_evidence_files_by_job(self, job_id):
        """Return mock evidence files matching CSV"""
        # Evidence_Share_Log_Shared_From_PrimeCorp BC_17_Sep_2025_08_22_25.csv
        # return [
        #     {
        #         'evidence_id': '6ce9ddf72ade4fec9aba84f2ab903c39',
        #         'source_case_title': 'PO-2025-99001',
        #     },
        #     {
        #         'evidence_id': '9a92309baf864d14aabe74b784532635',
        #         'source_case_title': 'PO-2025-99001',
        #     },
        #     {
        #         'evidence_id': 'd775effbada8426c9afd173061a11f56',
        #         'source_case_title': 'PO-2025-99001',
        #     },
        #     {
        #         'evidence_id': 'f6ae8d28d347475995fef24bb119cc5d',
        #         'source_case_title': 'PO-2025-99001',
        #     },
        #     {
        #         'evidence_id': '8ab7777c800b4d70bd093808f2e5b794',
        #         'source_case_title': 'PO-2025-99001',
        #     },
        #     {
        #         'evidence_id': '50ae0fb85cae41108bc560eba46c8f3a',
        #         'source_case_title': 'PO-2025-99001',
        #     },
        #     {
        #         'evidence_id': '601e0b30773f4899ba49dd30d7b2b283',
        #         'source_case_title': 'PO-2025-99001',
        #     },
        #     {
        #         'evidence_id': '1d6dd5a5ae3342c5b9c17cc4961ff4f0',
        #         'source_case_title': 'PO-2025-99001',
        #     },
        #     {
        #         'evidence_id': '11d423e24cac4847955dcfd646990b71',
        #         'source_case_title': 'PO-2025-99001',
        #     },
        # ]

        # Evidence_Share_Log_Shared_To_BC Prosecution Service - DEV_10_Jun_2025_12_35_44.csv
        return [
            {
                'evidence_id': '37b69b9ea73a4222a01ce106bf2d27bd',
                'evidence_file_id': 'file_37b69b9ea73a4222a01ce106bf2d27bd',
                'checksum': '0F74CA089651BD95D8CDDDFFCDA25E280918C14ED08EBFEB7D517E0D1369E726',
                'source_case_title': 'PO-2025-99001',
            },
            {
                'evidence_id': '65fa77687012420a99bb3cba56d82495',
                'evidence_file_id': 'file_65fa77687012420a99bb3cba56d82495',
                'checksum': 'A9D4AACD335EF0D5094D6683002895BDEB66C87DE69689D5F6A1DF73B7319923',
                'source_case_title': 'PO-2025-99001',
            },
            {
                'evidence_id': '1c6073aaf5374a88ae5037ea6ea133f2',
                'evidence_file_id': 'file_1c6073aaf5374a88ae5037ea6ea133f2',
                'checksum': 'F03292A6F37328999C734368920A1EC64CAEA1234478ED3888A04D562382B6D9',
                'source_case_title': 'PO-2025-99001',
            },
            {
                'evidence_id': '154deb088beb432d902ed8ba6960b63b',
                'evidence_file_id': 'file_154deb088beb432d902ed8ba6960b63b',
                'checksum': 'C8CB31F75E5260E016DE85A1FD6CF7528B62B1BAC369CB6922771D514DD02212',
                'source_case_title': 'PO-2025-99001',
            },
        ]

        # Evidence_Share_Log_Shared_To_PrimeCorp_BC_-_Police_12_Mar_2025_10_44_03.csv
        # return [
        #     {
        #         'evidence_id': '2c51c7f6a4694a5d9b6ee6f7b95a7252',
        #         'source_case_title': 'PO-2025-99001',
        #     },
        #     {
        #         'evidence_id': '7d540fa1620449869dcd04fbc531962c',
        #         'source_case_title': 'PO-2025-99001',
        #     },
        # ]

def main():
    # Paths
    # input_csv_path = Path('src/lambdas/axon-evidence-data-preparator/test_input/Evidence_Share_Log_Shared_From_PrimeCorp BC_17_Sep_2025_08_22_25.csv')
    input_csv_path = Path('src/lambdas/axon-evidence-data-preparator/test_input/Evidence_Share_Log_Shared_To_BC Prosecution Service - DEV_10_Jun_2025_12_35_44.csv')
    # input_csv_path = Path('src/lambdas/axon-evidence-data-preparator/test_input/Evidence_Share_Log_Shared_To_PrimeCorp_BC_-_Police_12_Mar_2025_10_44_03.csv')
    output_dir = Path('src/lambdas/axon-evidence-data-preparator/test_output')
    # append date time to output filename to avoid overwriting

    source_case_title = 'PO-2025-99001'
    timestamp = datetime.now().strftime('%y%m%d%H%M%S')
    output_csv_path = output_dir / f"ESL_{source_case_title}_{timestamp}.csv"
    
    # Create output directory
    output_dir.mkdir(exist_ok=True)
    
    # Setup
    job_id = '9ac2505b-cdd5-4fdf-a488-89a8a102df2a'
    agency_id_code = '412'
    
    logger = MockLogger()
    db_manager = MockDBManager(job_id)
    
    # Process
    try:
        processor = ESLProcessor(db_manager, agency_id_code, logger, 
                               base_url=None, bearer_token=None, agency_id=None)
        success, message = processor.process(
            str(input_csv_path),
            str(output_csv_path),
            job_id
        )
        
        print(f"\n✅ Success: {message}")
        print(f"📁 Output written to: {output_csv_path}")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()