import asyncio
import os
from logging import getLogger

import csv
import aiohttp

logger = getLogger(__name__)


class ChartMogulExtractor:
    base_url = "https://api.chartmogul.com/v1/"

    def __init__(self, account_token, secret_key, output_path=''):
        self.output_path = output_path
        self.auth = aiohttp.BasicAuth(account_token, secret_key)

    async def get_data_from_page(self, endpoint, page_number):
        url = f"{self.base_url}{endpoint}"
        async with aiohttp.ClientSession(auth=self.auth) as session:
            async with session.get(url, params={"page": page_number}) as res:
                content = await res.json()
                return content

    async def extract_customers_page(self, page_number, writer, semaphore):
        async with semaphore:
            logger.info(f"Start extraction of customers page {page_number}")
            data = await self.get_data_from_page("customers", page_number)
        writer.writerows(data["entries"])
        logger.info(f"Finish extraction of customers page {page_number}")

    async def extract_invoices_page(self, page_number, invoice_writer, line_items_writer, transaction_writer, semaphore):
        async with semaphore:
            logger.info(f"Start extraction of invoices page {page_number}")
            data = await self.get_data_from_page("invoices", page_number)
        for invoice in data["invoices"]:
            invoice_uuid = invoice["uuid"]
            if invoice["transactions"]:
                transaction_writer.writerows([{"invoice_uuid": invoice_uuid, **item} for item in invoice["transactions"]])
            if invoice["line_items"]:
                line_items_writer.writerows([{"invoice_uuid": invoice_uuid, **item} for item in invoice["line_items"]])
            invoice_writer.writerow(invoice)
        logger.info(f"Finish extraction of invoices page {page_number}")

    async def get_page_count(self, endpoint):
        data = await self.get_data_from_page(endpoint, 1)
        return data["total_pages"]

    async def extract(self):
        semaphore = asyncio.Semaphore(20)
        logger.info("Start extraction")
        customers_fields = ["id", "uuid", "external_id", "name", "email", "status", "customer-since", "attributes",
                            "data_source_uuid", "data_source_uuids", "external_ids", "company", "country", "state",
                            "city", "zip", "lead_created_at", "free_trial_started_at", "address", "mrr", "arr",
                            "billing-system-url", "chartmogul-url", "billing-system-type", "currency", "currency-sign"]
        invoices_fields = ["uuid", "external_id", "date", "due_date", "currency", "customer_uuid"]
        transactions_fields = ["invoice_uuid", "uuid", "external_id", "type", "date", "result"]
        line_items_fields = ["invoice_uuid", "subscription_uuid", "subscription_external_id", "prorated",
                             "service_period_start", "service_period_end", "uuid", "external_id", "type",
                             "amount_in_cents", "quantity", "discount_code", "discount_amount_in_cents",
                             "tax_amount_in_cents", "transaction_fees_in_cents", "account_code", "plan_uuid",
                             "transaction_fees_currency", "discount_description", "event_order"]

        customers_page_count = await self.get_page_count("customers")
        invoices_page_count = await self.get_page_count("invoices")

        customer_file = open(os.path.join(self.output_path, f"customers.csv"), "w", encoding="utf-8", newline="")
        invoice_file = open(os.path.join(self.output_path, f"invoices.csv"), "w", encoding="utf-8", newline="")
        transaction_file = open(os.path.join(self.output_path, f"transactions.csv"), "w", encoding="utf-8", newline="")
        line_items_file = open(os.path.join(self.output_path, f"invoice_line_items.csv"), "w", encoding="utf-8", newline="")

        customer_writer = csv.DictWriter(customer_file, fieldnames=customers_fields, dialect="unix")
        customer_writer.writeheader()
        customers_tasks = [asyncio.create_task(self.extract_customers_page(page_number, customer_writer, semaphore)) for page_number in
                 range(1, customers_page_count+1)]
        invoice_writer = csv.DictWriter(invoice_file, fieldnames=invoices_fields, dialect="unix", extrasaction="ignore")
        transaction_writer = csv.DictWriter(transaction_file, fieldnames=transactions_fields, dialect="unix", extrasaction="ignore")
        line_items_writer = csv.DictWriter(line_items_file, fieldnames=line_items_fields, dialect="unix", extrasaction="ignore")
        invoice_writer.writeheader()
        transaction_writer.writeheader()
        line_items_writer.writeheader()
        invoices_tasks = [asyncio.create_task(self.extract_invoices_page(page_number, invoice_writer, line_items_writer, transaction_writer, semaphore)) for page_number in
                 range(1, invoices_page_count+1)]

        tasks = customers_tasks + invoices_tasks
        await asyncio.gather(*tasks)
        customer_file.close()
        invoice_file.close()
        transaction_file.close()
        line_items_file.close()
