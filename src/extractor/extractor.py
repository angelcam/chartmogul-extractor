import asyncio
import os
from logging import getLogger

import aiocsv
import aiofiles
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

    async def extract_customers_page(self, page_number, writer):
        logger.info(f"Start extraction of customers page {page_number}")
        data = await self.get_data_from_page("customers", page_number)
        await writer.writerows(data["entries"])
        logger.info(f"Finish extraction of customers page {page_number}")

    async def extract_invoices_page(self, page_number, invoice_writer, line_items_writer, transaction_writer):
        logger.info(f"Start extraction of invoices page {page_number}")
        data = await self.get_data_from_page("invoices", page_number)
        for invoice in data["invoices"]:
            invoice_uuid = invoice["uuid"]
            await transaction_writer.writerows([{"invoice_uuid": invoice_uuid, **item} for item in invoice["transactions"]])
            await line_items_writer.writerows([{"invoice_uuid": invoice_uuid, **item} for item in invoice["line_items"]])
            await invoice_writer.writerow(invoice)
        logger.info(f"Finish extraction of invoices page {page_number}")

    async def get_page_count(self, endpoint):
        data = await self.get_data_from_page(endpoint, 1)
        return data["total_pages"]

    async def extract(self):
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
        async with aiofiles.open(os.path.join(self.output_path, f"customers.csv"), "w", encoding="utf-8",
                                 newline="") as cuf, aiofiles.open(os.path.join(self.output_path, f"invoices.csv"), "w",
                                                                   encoding="utf-8", newline="") as inf, aiofiles.open(
                os.path.join(self.output_path, f"transactions.csv"), "w", encoding="utf-8",
                newline="") as trf, aiofiles.open(os.path.join(self.output_path, f"invoice_line_items.csv"), "w",
                                                  encoding="utf-8", newline="") as lif:
            customer_writer = aiocsv.AsyncDictWriter(cuf, fieldnames=customers_fields, dialect="unix")
            await customer_writer.writeheader()
            customers_tasks = [asyncio.create_task(self.extract_customers_page(page_number, customer_writer)) for page_number in
                     range(1, customers_page_count+1)]

            invoice_writer = aiocsv.AsyncDictWriter(inf, fieldnames=invoices_fields, dialect="unix", extrasaction="ignore")
            transaction_writer = aiocsv.AsyncDictWriter(trf, fieldnames=transactions_fields, dialect="unix", extrasaction="ignore")
            line_items_writer = aiocsv.AsyncDictWriter(lif, fieldnames=line_items_fields, dialect="unix", extrasaction="ignore")
            await invoice_writer.writeheader()
            await transaction_writer.writeheader()
            await line_items_writer.writeheader()
            invoices_tasks = [asyncio.create_task(self.extract_invoices_page(page_number, invoice_writer, line_items_writer, transaction_writer)) for page_number in
                     range(1, invoices_page_count+1)]

            tasks = customers_tasks + invoices_tasks
            await asyncio.gather(*tasks)