from django.apps import apps
from django.contrib.contenttypes.fields import GenericForeignKey
from django.core.exceptions import ObjectDoesNotExist


class CombinedForeignKey(GenericForeignKey):
    def __init__(self, related_model_name, *args, **kwargs):
        del kwargs["related_name"]
        del kwargs["db_constraint"]
        self.related_model_name = related_model_name
        super().__init__(*[], **kwargs)

    def __get__(self, instance, cls=None):
        if instance is None:
            return self

        rel_obj = self.get_cached_value(instance, default=None)
        if rel_obj is not None:
            return rel_obj
        try:
            rel_obj = self.get_related_object(instance=instance, using=instance._state.db)
        except ObjectDoesNotExist:
            pass
        self.set_cached_value(instance, rel_obj)
        return rel_obj

    def contribute_to_class(self, cls, name, **kwargs):
        super().contribute_to_class(cls, name, **kwargs)
        self.attname = name

    def get_related_model(self):
        app_label, model_name = self.related_model_name.split(".")
        return apps.get_model(app_label=app_label, model_name=model_name)

    def get_related_object(self, instance, using):
        local_ct_id = f"{self.name}__{self.ct_field}"
        local_fk_val = f"{self.name}__{self.fk_field}"
        ct_id = getattr(instance, local_ct_id, None)
        fk_val = getattr(instance, local_fk_val)

        return self.get_related_model().objects.using(using).filter(**{self.ct_field: ct_id, self.fk_field: fk_val}).first


        # app_label, model_name = self.related_model_name.split('.')
        # relation_model = apps.get_model(
        #     app_label=app_label,
        #     model_name=model_name)
        # return relation_model.buurt.field.remote_field.model.objects.raw("SELECT b.* FROM bagh_buurt b INNER JOIN gebieden_ggwgebied_buurt rel ON (b.volgnummer = rel.volgnummer AND b.identificatie=rel.identifier) WHERE rel.ggwgebied_id = 1")
