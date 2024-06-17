from pyart.io import uf
from datetime import timedelta
from datetime import datetime
from numpy import ma
from gzip import open as gzip_open

class Reader():
    """Reader that reads all data from a set of UF Radar Files.
    """
    def __init__(self, file_path):
        self.uf_file = file_path

    def _read_radar(self):
        """Read the radar file and return some important values.

        :return: The radar data including some important values.
        :rtype: Tuple
        """
        uf_filename = self.uf_file
        if uf_filename.endswith('.gz'):
            with gzip_open(uf_filename, 'rb') as unzipped_file:
                radar = uf.read_uf(
                    unzipped_file,
                    file_field_names=True
                )
        else:
            radar = uf.read_uf(
                uf_filename,
                file_field_names=True
            )
        sweep_start_ray_idx = radar.sweep_start_ray_index['data'][:]
        sweep_end_ray_idx = radar.sweep_end_ray_index['data'][:]

        cz = (radar.fields['CZ']['data'][:])
        dr = (radar.fields['DR']['data'][:])
        rh = (radar.fields['RH']['data'][:])
        
        fh = (radar.fields['FH']['data'][:])
        dm = (radar.fields['DM']['data'][:])


        # Lucy Wang added the following command lines on July 23, 2018
        gate_latitude = radar.gate_latitude['data'][:]
        gate_longitude = radar.gate_longitude['data'][:]
        gate_altitude = radar.gate_altitude['data'][:]
        # -------------------------------------------------------------

        # Get time in %Y-%m-%d%H:%M:%SZ format
        full_time = radar.time['units']\
            .replace('since', '')\
            .replace('seconds', '')
        full_time = full_time.replace(' ', '')
        full_time = full_time.replace('T', '')
        full_time = datetime.strptime(full_time, '%Y-%m-%d%H:%M:%SZ')

        return (
            sweep_start_ray_idx,
            sweep_end_ray_idx,
            cz,
            dr,
            rh,
            fh,
            dm,
            gate_latitude,
            gate_longitude,
            gate_altitude,
            full_time,
            radar,
        )

    def read_data(self):
        """Generator function that generates each datum from the radar file
        sequentially.
        """
        index = 0
        (
            sweep_start_ray_idx,
            sweep_end_ray_idx,
            CZ,
            DR,
            RH,
            FH,
            DM,
            gate_latitude,
            gate_longitude,
            gate_altitude,
            full_time,
            radar
        ) = self._read_radar()

        # sweep by sweep; 20 sweeps per rhi_a file (over ocean)
        for ii in range(0, radar.nsweeps):
            # Calculate the start and end time for this sweep
            idx0 = sweep_start_ray_idx[ii]
            idx1 = sweep_end_ray_idx[ii]

            # ray by ray; 226 rays per sweep
            for ray in range(idx0, idx1 + 1):
                tmp_cz = CZ[ray, :]
                tmp_dr = DR[ray, :]
                tmp_rh = RH[ray, :]
                tmp_fh = FH[ray, :]
                tmp_dm = DM[ray, :]
                tmp_time_ray = (full_time + timedelta(
                    seconds=float(radar.time['data'][ray])
                )).strftime('%Y-%m-%dT%H:%M:%SZ')
                tmp_gate_lat = gate_latitude[ray, :]
                tmp_gate_lon = gate_longitude[ray, :]
                tmp_gate_alt = gate_altitude[ray, :]

                # check CZ values gate by gate; 1081 gates per ray
                for gate in range(0, len(tmp_cz)):
                    if tmp_cz[gate] is not ma.masked:
                        row_dict = {
                            'timestamp': tmp_time_ray,
                            'lat': round(float(tmp_gate_lat[gate]), 4),
                            'lon': round(float(tmp_gate_lon[gate]), 4),
                            'height': float(tmp_gate_alt[gate]),
                            'CZ': float(tmp_cz[gate]),
                            'DR': float(tmp_dr[gate]),
                            'RH': float(tmp_rh[gate]),
                            'FH': float(tmp_fh[gate]),
                            'DM': float(tmp_dm[gate]),
                        }
                        yield row_dict
                        index += 1
