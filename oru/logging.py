import os

class CSVLog:
    def __init__(self, filename, mode='x', index=None):
        """
        Very basic CSV logger.
        :param filename: File path to log to
        :param mode: Single character with one of the following values
            'a' - append to the end of the log
            'x' - create new file, fail if exists
            'w' - create new file, truncate existing.
        :param index: Name of column name to use as an index (similar to pandas Dataframes)

        The resulting object is callable using keywork=value arguments for fieldname=value in the log
        >>> log = CSVLog('log.csv', index='epoch')
        >>> log(epoch=0, foo='yes', bar=False, loss=0.9)
        >>> log(epoch=1, foo='yes', bar=True, loss=0.8)
        >>> log(foo='yes', loss=0.7, bar=False, epoch=2)
        >>> log(epoch=3, foo='no', bar=False, loss=0.1)
        >>> log.close()
        >>> %cat log.csv
            epoch,foo,bar,loss
            0,'yes',False,0.9
            1,'yes',True,0.8
            2,'yes',False,0.7
            3,'no',False,0.1

        """

        if mode in ('x', 'w'):
            self._fp = open(filename, mode=mode+'+')
        elif mode == 'a':
            if os.path.exists(filename):
                self._fp = open(filename, mode='r+')
            else:
                mode = 'x'
                self._fp = open(filename, mode='w+')
        else:
            raise ValueError(f'invalid mode: {mode} - must be one of a,x,w')

        self._fname = filename

        if mode=='a':
            self._fp.seek(0,0)
            header = self._fp.readline().strip('\n').split(',')
            self._columns = set(header)
            self._columns_order = header
            self._fmt_str = ",".join("{" + c + "!s}" for c in self._columns_order)
            self._fp.seek(0,2)

        else:
            if index is not None:
                self._columns = {index}
                self._columns_order = [index]
                self._fmt_str = "{" + index + "!s}"

            else:
                self._columns = set()
                self._columns_order = []
                self._fmt_str = ""

            self._fp.write("HEADER\n")
            self._fp.flush()



    def __call__(self, **kwargs):
        kw = {k: "" for k in self._columns}
        kw.update(kwargs)

        for key in kw:
            if key not in self._columns:
                self._columns.add(key)
                self._columns_order.append(key)
                if len(self._fmt_str) > 0:
                    self._fmt_str += ",{" + key + "!s}"
                else:
                    self._fmt_str = "{" + key + "!s}"

        self._fp.write(self._fmt_str.format_map(kw) + "\n")
        self._fp.flush()


    def update_header(self):
        """Update the header in the file, add empty fields if required.  The entire file is re-."""
        self._fp.seek(0,0)
        lines = self._fp.readlines()
        lines[0] = ",".join(self._columns_order) + "\n"
        num_cols = len(self._columns_order)
        for i in range(1, len(lines)):
            lines[i] = lines[i][:-1] + "," * (num_cols - lines[i].count(",") - 1) + "\n"

        self._fp.seek(0,0)
        self._fp.writelines(lines)
        self._fp.truncate(self._fp.tell())
        self._fp.flush()


    def close(self):
        if hasattr(self, "_fp") and self._fp is not None:
            self.update_header()
            self._fp.close()
            self._fp = None


class TablePrinter:
    def __init__(self, header, float_prec=2, justify='>', sep=' ', min_col_width=4, col_widths=None,
                 delay_header_print=False):
        self.min_col_width = min_col_width
        if col_widths is None:
            self.col_widths = list(map(lambda x : max(len(x), self.min_col_width), header))
        else:
            if len(col_widths) != len(header):
                raise ValueError("length of col_widths does not match length of header")
            self.col_widths = col_widths

        self.float_prec = float_prec
        self.sep = sep
        self.ncols = len(self.col_widths)
        self.justify = '^'
        self.header = header
        self._header_printed = ~delay_header_print
        if not delay_header_print:
            self.print_line(*header)
        self.justify = justify


    def format_float(self, val, col_idx):
        fmt_str = f'{{:{self.justify}{self.col_widths[col_idx]:d}.{self.float_prec:d}f}}'
        return fmt_str.format(val)

    def format_string(self, val, col_idx):
        fmt_str = f'{{:{self.justify}{self.col_widths[col_idx]:d}s}}'
        return fmt_str.format(val)

    def format_int(self, val, col_idx):
        fmt_str = f'{{:{self.justify}{self.col_widths[col_idx]:d}d}}'
        return fmt_str.format(val)

    def format_val(self, val, col_idx):
        if isinstance(val, float):
            return self.format_float(val, col_idx)
        elif isinstance(val, int):
            return self.format_int(val, col_idx)
        else:
            return self.format_string(str(val), col_idx)

    def format_line(self, *vals):
        if len(vals) < self.ncols:
            vals = vals + tuple('' for _ in range(self.ncols - len(vals)))
        return self.sep.join(self.format_val(v,i) for i,v in enumerate(vals))

    def print_line(self, *vals):
        if not self._header_printed:
            self._header_printed = True
            self.print_line(self.header)
        print(self.format_line(*vals[:self.ncols]))
        if len(vals) > self.ncols:
            self.print_line(*vals[self.ncols:])


