from csv import DictReader, writer
from collections import defaultdict
import ast
import json
import os


def convert_to_python_dict(csv_reader: DictReader) -> list:
    return [
        {key: ast.literal_eval(row[key])["S"] for key in row if key}
        for row in csv_reader
    ]


def process_data_dicts(data_dicts: list) -> dict:
    day_counts = defaultdict(int)
    res_total, no_res = 0, 0
    for res in data_dicts:
        res_total += 1
        if not res["job_results"]:
            no_res += 1
            continue

        job_request = json.loads(res["job_request"])
        job_type = res["job_type"]
        job_results = json.loads(res["job_results"])

        if job_type == "address":
            day_counts["address_total_requests"] += 1
            if "ncoa" in job_request.get("process", []):
                day_counts["address_ncoa"] += 1
            if "pcoa" in job_request.get("process", []):
                day_counts["address_pcoa"] += 1
            if "fp" in job_request.get("process", []):
                day_counts["address_fp"] += 1
                if job_results:
                    for e in job_results.get("append", {}).get("phone", []):
                        day_counts["address_fp_found_phones"] += 1
                        found_type = e["type"].lower() or "unknown_type"
                        if found_type in ["unknown_type", "voip", "other"]:
                            day_counts["address_fp_found_phone_landline_phones"] += 1

                        day_counts[f"address_fp_found_phone_{found_type}_phones"] += 1
                        if e.get("connected") is True:
                            day_counts["address_fp_connected_phones"] += 1
            if "fe" in job_request.get("process", []):
                day_counts["address_fe"] += 1
                if job_results:
                    for e in job_results.get("append", {}).get("email", []):
                        day_counts["address_fe_found_emails"] += 1
                        if e.get("is_valid"):
                            day_counts["address_fe_valid_emails"] += 1

        if job_type == "phone":
            day_counts["phone_total_requests"] += 1
            if job_results.get("phone"):
                day_counts["phone_found_input"] += 1
                if job_results.get("phone", {}).get("connected"):
                    day_counts["phone_input_connecteds"] += 1
            if "fd" in job_request.get("process", []):
                day_counts["phone_fd"] += 1
            if "fp" in job_request.get("process", []):
                day_counts["phone_fp"] += 1
                if job_results:
                    for e in job_results.get("append", {}).get("phone", []):
                        day_counts["phone_fp_found_phone"] += 1
                        found_type = e["type"].lower()
                        day_counts[f"phone_fp_found_phone_{found_type}_phones"] += 1
                        if e.get("connected") is True:
                            day_counts["phone_fp_connected_phones"] += 1
            if "fe" in job_request.get("process", []):
                day_counts["phone_fe"] += 1
                if job_results:
                    for e in job_results.get("append", {}).get("email", []):
                        day_counts["phone_fe_found_emails"] += 1
                        if e.get("is_valid"):
                            day_counts["phone_fe_valid_emails"] += 1
            if "dn" in job_request.get("process", []):
                day_counts["phone_dn"] += 1
                if job_results:
                    if job_results.get("phone").get("connected") is not None:
                        day_counts["phone_dn_matched"] += 1

        if job_type == "email":
            day_counts["email_total_requests"] += 1
            if job_results:
                day_counts["email_found_input"] += 1
                if job_results.get("email", {}).get("is_valid"):
                    day_counts["email_found_input_valid"] += 1

        if job_type == "cima" and "propensityscore" not in job_request:
            day_counts["cima"] += 1

        if job_type == "cima_select" and "propensityscore" not in job_request:
            day_counts["cima_select"] += 1

        if job_type == "cima_cert" and "propensityscore" not in job_request:
            day_counts["cima_cert"] += 1

    day_counts["reverse_phone_append_no_code"] = (
        day_counts["phone_total_requests"]
        - day_counts["phone_fd"]
        - day_counts["phone_dn"]
    )
    if no_res > 0:
        print(f"Failed to get jobs for {no_res} out of {res_total}")
    return day_counts


class DataProcessor:
    def __init__(self, result_file_name: str):
        self.filenames: list = []
        self.result_file_name: str = result_file_name
        self.date_to_data: dict = defaultdict(list)
        self.date_to_count: dict = defaultdict(dict)

    def get_filenames_by_prefix(self, prefix, directory=".") -> None:
        self.filenames = [
            filename
            for filename in os.listdir(directory)
            if filename.startswith(prefix)
        ]

    def read_initial_csv_files(self) -> None:
        for filename in self.filenames:
            with open(filename, mode="r", newline="") as file:
                self.group_by_date(convert_to_python_dict(DictReader(file)))

    def group_by_date(self, data_dicts: list) -> None:
        for data_dict in data_dicts:
            self.date_to_data[data_dict["creation_yearmonth"]].append(data_dict)

    def count_by_date(self) -> None:
        for date, data_dicts in self.date_to_data.items():
            self.date_to_count[date] = process_data_dicts(data_dicts)

    def create_csv_report(self) -> None:
        column_names = {
            "cima": "CIMA",
            "cima_select": "Cima Select",
            "cima_cert": "Cima Cert",
            "cima_estimated_credit": "Cima Estimated Credit",
            "cima_select_estimated_credit": "Cima Select Estimated Credit",
            "cima_cert_estimated_credit": "Cima Cert Estimated Credit",
            "address_ncoa": "Address NCOA",
            "address_pcoa": "Address PCOA",
            "address_fp_found_phone_landline_phones": "Address FP Found Phone Landline Phones",
            "address_fp_found_phone_wireless_phones": "Address FP Found Phone Wireless Phones",
            "phone_fp_found_phone_wireless_phones": "Phone FP Found Phone Wireless Phones",
            "phone_fp_found_phone_landline_phones": "Phone FP Found Phone Landline Phones",
            "address_fe_valid_emails": "Address FE Valid Emails",
            "phone_fe_valid_emails": "Phone FE Valid Emails",
            "phone_dn": "Phone Dn",
            "phone_dn_matched": "Phone Dn Matched",
            "phone_fd": "Phone FD",
            "email_found_input_valid": "Email Found Input Valid",
            "reverse_phone_append_no_code": "Reverse Phone Append No Code",
        }

        results: list = []

        for date, count_dict in self.date_to_count.items():
            result = {"Date": date}
            for field, column_name in column_names.items():
                result[column_name] = count_dict.get(field, 0)
            results.append(result)

        self.write_data_to_result_csv(results)

    def write_data_to_result_csv(self, results_list) -> None:
        with open(self.result_file_name, mode="w", newline="") as file:
            wr = writer(file)
            header_exists = False

            for rs in sorted(results_list, key=lambda x: x["Date"]):
                if not header_exists:
                    wr.writerow(rs.keys())
                    header_exists = True
                wr.writerow(rs.values())


if __name__ == "__main__":
    for prefix in ["developers@rewn.com", "info@datascore.ai", "koby@leadrilla.com"]:
        processor = DataProcessor(result_file_name=f"{prefix}.csv")
        processor.get_filenames_by_prefix(prefix)
        processor.read_initial_csv_files()
        processor.count_by_date()
        processor.create_csv_report()
        print(
            f'Report was created successfully! Check "{prefix}.csv" file in your current dir.'
        )
