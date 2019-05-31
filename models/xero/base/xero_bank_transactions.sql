select type as bank_transaction_type
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
	, bankaccount__code as bank_account_code
	, bankaccount__name as bank_account_name
	, bankaccount__type as bank_account_type
	, bankaccount__updateddateutc as bank_account_updateddateutc
	, bankaccount__reportingcodename as bank_account_reportingcodename
	, bankaccount__systemaccount as bank_account_systemaccount
	, bankaccount__bankaccounttype as bank_account_bankaccounttype
	, bankaccount__taxtype as bank_account_taxtype
	, bankaccount__description as bank_account_description
	, bankaccount__class as bank_account_class
	, bankaccount__accountid as bank_account_accountid
	, bankaccount__bankaccountnumber as bank_account_bankaccountnumber
	, bankaccount__status as bank_account_status
	, bankaccount__showinexpenseclaims as bank_account_showinexpenseclaims
	, bankaccount__currencycode as bank_account_currencycode
	, bankaccount__reportingcode as bank_account_reportingcode
	, bankaccount__enablepaymentstoaccount as bank_account_enablepaymentstoaccount
	, bankaccount__hasattachments as bank_account_hasattachments
	, isreconciled
	, {{ expand_date(ts_field='date') }}
	, datestring
	, reference
	, currencycode as currency_code
	, currencyrate as currency_rate
	, url
	, status
	, lineamounttypes as line_amount_types
	, subtotal
	, totaltax as total_tax
	, total
	, banktransactionid as bank_transaction_id
	, prepaymentid
	, overpaymentid
	, updateddateutc
	, hasattachments
	, externallinkprovidername
from tap_xero.bank_transactions
