import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app import create_app
from app.services.graph_service import GraphService
from app.services.import_service import ImportService
from app.services.nlp_service import NlpService


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, required=True)
    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        raise FileNotFoundError(file_path)

    app = create_app()
    with app.app_context():
        service = ImportService(NlpService(), GraphService())
        rows = service.load_json_file(str(file_path))
        report = service.import_reviews(rows)
        print(
            f"total={report.total} imported={report.imported} "
            f"updated={report.updated} failed={report.failed}"
        )
        if report.errors:
            print("sample errors:")
            for err in report.errors[:5]:
                print(f"- {err}")


if __name__ == "__main__":
    main()
