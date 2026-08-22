# Bambuddy Printer Plugin
#
# Keep the large upstream-derived driver.py merge-friendly: package import runs
# before ``app.plugins.bambuddy.driver`` is returned to FilaMan's plugin manager,
# so replacing the module's exported Driver with this small subclass layers our
# durable consumption protocol on top without copying the base driver.
from app.plugins.bambuddy import driver as _driver_module
from app.plugins.bambuddy.durable_usage import DurableUsageMixin


class Driver(DurableUsageMixin, _driver_module.Driver):
    """Bambuddy driver with restart/reconnect-safe consumption replay."""


_driver_module.Driver = Driver

__all__ = ["Driver"]
