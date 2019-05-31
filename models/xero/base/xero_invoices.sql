select type as invoice_type
	, contact__contactid as contact_id
	, contact__contactnumber as contact_number
	, contact__accountnumber as contact_account_number
	, contact__contactstatus as contact_contact_status
	, contact__name as contact_name
	, contact__firstname as contact_firstname
	, contact__lastname as contact_lastname
	, contact__emailaddress as contact_emailaddress
	, contact__skypeusername as contact_skypeusername
	, contact__bankaccountdetails as contact_bankaccountdetails
	, contact__taxnumber as contact_taxnumber
	, contact__accountsreceivabletaxtype as contact_accountsreceivabletaxtype
	, contact__accountspayabletaxtype as contact_accountspayabletaxtype
	, contact__issupplier as contact_issupplier
	, contact__iscustomer as contact_iscustomer
	, contact__defaultcurrency as contact_defaultcurrency
	, contact__updateddateutc as contact_updateddateutc
	, contact__xeronetworkkey as contact_xeronetworkkey
	, contact__salesdefaultaccountcode as contact_salesdefaultaccountcode
	, contact__purchasesdefaultaccountcode as contact_purchasesdefaultaccountcode
	, contact__trackingcategoryname as contact_trackingcategoryname
	, contact__trackingcategoryoption as contact_trackingcategoryoption
	, contact__paymentterms__sales__day as contact_paymentterms__sales__day
	, contact__paymentterms__sales__type as contact_paymentterms__sales__type
	, contact__paymentterms__bills__day as contact_paymentterms__bills__day
	, contact__paymentterms__bills__type as contact_paymentterms__bills__type
	, contact__website as contact_website
	, contact__brandingtheme__createddateutc as contact_brandingtheme__createddateutc
	, contact__brandingtheme__sortorder as contact_brandingtheme__sortorder
	, contact__brandingtheme__name as contact_brandingtheme__name
	, contact__brandingtheme__brandingthemeid as contact_brandingtheme__brandingthemeid
	, contact__batchpayments__details as contact_batchpayments__details
	, contact__batchpayments__reference as contact_batchpayments__reference
	, contact__batchpayments__code as contact_batchpayments__code
	, contact__batchpayments__bankaccountnumber as contact_batchpayments__bankaccountnumber
	, contact__batchpayments__bankaccountname as contact_batchpayments__bankaccountname
	, contact__discount as contact_discount
	, contact__balances__accountsreceivable__outstanding as contact_balances__accountsreceivable__outstanding
	, contact__balances__accountsreceivable__overdue as contact_balances__accountsreceivable__overdue
	, contact__balances__accountspayable__outstanding as contact_balances__accountspayable__outstanding
	, contact__balances__accountspayable__overdue as contact_balances__accountspayable__overdue
	, contact__hasattachments as contact_hasattachments
	, contact__hasvalidationerrors as contact_hasvalidationerrors
	, {{ expand_date(ts_field='date') }}
	, duedate as due_date
	, status
	, lineamounttypes as line_amount_types
	, subtotal
	, totaltax as total_tax
	, total
	, totaldiscount as total_discount
	, updateddateutc as updated_date
	, currencycode as currency_code
	, currencyrate as currency_rate
	, invoiceid as invoice_id
	, invoicenumber as invoice_number
	, reference
	, brandingthemeid
	, url
	, senttocontact
	, expectedpaymentdate
	, expectedpaymentdatestring
	, plannedpaymentdate
	, plannedpaymentdatestring
	, hasattachments
	, amountdue as amount_due
	, amountpaid as amount_paid
	, fullypaidondate
	, amountcredited
	, duedatestring
	, isdiscounted
	, haserrors
	, datestring
from tap_xero.invoices
