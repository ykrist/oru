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


