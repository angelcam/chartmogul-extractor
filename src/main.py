import asyncio

from keboola.component import CommonInterface

from extractor.extractor import ChartMogulExtractor

REQUIRED_PARAMETERS = ["account_token", "#secret_key"]

if __name__ == "__main__":
    ci = CommonInterface()
    ci.validate_configuration_parameters(REQUIRED_PARAMETERS)

    account_token = ci.configuration.parameters.get("account_token")
    secret_key = ci.configuration.parameters.get("#secret_key")
    plan_table = ci.create_out_table_definition('plans.csv', primary_key=['external_id'])
    customer_table = ci.create_out_table_definition('customers.csv', primary_key=['external_id'])
    invoice_table = ci.create_out_table_definition('invoices.csv', primary_key=['external_id'])
    transaction_table = ci.create_out_table_definition('transactions.csv', primary_key=['external_id'])
    invoice_line_item_table = ci.create_out_table_definition('invoice_line_items.csv', primary_key=['external_id'])
    extractor = ChartMogulExtractor(account_token,
                                    secret_key,
                                    plans_file_path=plan_table.full_path,
                                    customer_file_path=customer_table.full_path,
                                    invoice_file_path=invoice_table.full_path,
                                    transaction_file_path=transaction_table.full_path,
                                    line_items_file_path=invoice_line_item_table.full_path
                                    )
    asyncio.run(extractor.extract())

    ci.write_manifest(plan_table)
    ci.write_manifest(customer_table)
    ci.write_manifest(invoice_table)
    ci.write_manifest(transaction_table)
    ci.write_manifest(invoice_line_item_table)
