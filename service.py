from collections import defaultdict
import os
from dotenv import load_dotenv
from typing import Literal
import httpx
from enum import StrEnum
import logging


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


class Url(StrEnum):
    BOBDAY_GET = os.getenv("BOBDAY_GET")
    BOBDAY_POST = os.getenv("BOBDAY_POST")
    TEST = "http://127.0.0.1:8001/test"


class HTTPProcessor:
    __user, __password = os.getenv("AUTH_USER"), os.getenv("AUTH_PASSWORD")
    auth = (__user, __password)


class DataReceiver(HTTPProcessor):
    def __init__(self) -> None:
        self.data = None

    def get(self) -> dict:
        try:
            response = httpx.get(url := Url.BOBDAY_GET, auth=super().auth)
            if (code := response.status_code) < 300:
                self.data = response.json()
                return self.data
            logging.error(f"Request error. Status code: {code}")
        except httpx.ConnectError as e:
            logging.error(f"Connection error to `{url}`")
            raise e
        except:
            ...


class DataProcessor:
    def __init__(self, *, data: dict) -> None:
        self.data = data.get("employees") if data else {}

    def process(self) -> dict | Literal["Data is empty"]:
        if not self.data:
            return "Data is empty"

        department_employees, result_dct = defaultdict(list), dict()

        for employee in self.data:
            department = employee.get("department")
            department_id, department_name = (
                department["id"],
                department["department_name"],
            )
            employee_ = {"name": employee.get("name"), "age": employee.get("age")}
            department_employees[department_id].append(employee_)
            result_dct.update(
                {
                    department_id: {
                        "department_name": department_name,
                        "employees": department_employees[department_id],
                    }
                }
            )
        return result_dct


class DataSender(HTTPProcessor):
    def __init__(self, *, data_sent: dict) -> None:
        self.data = data_sent

    def send(self) -> None:
        try:
            response = httpx.post(Url.BOBDAY_POST, json=self.data, auth=super().auth)
            logging.info(
                "---> Ok. Delivered"
                if (code := response.status_code) < 300
                else f"---> Send error: {code}"
            )
        except:
            ...

    def save(self) -> None:
        with open("result.json", "w") as f:
            f.write(str(self.data))
        logging.info("---> Ok, Saved")


def main():
    data = DataReceiver().get()
    processed_data = DataProcessor(data=data).process()
    logging.info(processed_data)
    ds = DataSender(data_sent=processed_data)
    ds.send()
    ds.save()


if __name__ == "__main__":
    main()
