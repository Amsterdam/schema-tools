from __future__ import annotations

from django.dispatch import Signal

#: Signal all listening apps that the models were removed.
dynamic_models_removed = Signal()
