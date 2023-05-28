import abc


class NSEDownloadMap(abc.ABC):
    """
    Abstract class which serves as a differentiator
    for extracting all download related urls/configs.
    """

    @property
    def nse_download_eq_bhavcopy(self):
        return (
                self.nse_download_domain
                + self.loaded_dict["Market"]["NSE"]["DOWNLOADS"]["EQBhavCopy"][
                    "url"
                ]
        )

    @property
    def nse_download_eq_bhavcopy_dwnd_path(self):
        return (
                self.nse_download_domain
                + self.loaded_dict["Market"]["NSE"]["DOWNLOADS"]["EQBhavCopy"][
                    "file_path"
                ]
        )

    @property
    def nse_download_fo_mktlots(self):
        return self.loaded_dict["Market"]["NSE"]["DOWNLOADS"]["FO-MKLOTS"][
            "url"
        ]

    @property
    def nse_download_fo_bhavcopy(self):
        return self.nse_download_domain + self.loaded_dict["Market"]["NSE"][
            "DOWNLOADS"]["FOBhavCopy"]["url"]

    @property
    def nse_download_fo_secban(self):
        return self.nse_download_domain + self.loaded_dict["Market"]["NSE"][
            "DOWNLOADS"]["FOSecBan"]["url"]
