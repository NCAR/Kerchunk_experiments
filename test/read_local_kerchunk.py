import fsspec
import sys



if len(sys.argv) == 1:
    print(f'Usage {sys.argv[0]} [filename]')
    exit(1)

test_file = sys.argv[1]
fs = fsspec.filesystem('reference', fo=test_file)
m = fs.get_mapper('')
ds = xarray.open_dataset(m, engine='zarr', backend_kwargs={'consolidated':False})
print(ds)
