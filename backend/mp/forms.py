from django import forms
from django.core.exceptions import ValidationError
from django.db.models import Q
from .models import Shop, APIKey, Selfbuy


class ShopForm(forms.ModelForm):

    name = forms.CharField(
        required=True,
        label="Название",
        help_text="Название магазина",
        error_messages={"required": "Укажите название"},
    )

    class Meta:
        model = Shop
        fields = ["shop_token", "name", "is_active"]
        labels = {
            "shop_token": "Маркетплейс",
            "is_active": "Включен?",
        }


class APIKeyForm(forms.ModelForm):

    name = forms.CharField(
        required=True,
        label="Название",
        help_text="Название токена",
        error_messages={"required": "Укажите название"},
    )
    client_id = forms.CharField(required=True, label="Client Id")
    client_secret = forms.CharField(
        required=True,
        label="Client Secret",
        # widget=forms.PasswordInput(render_value=True),
    )

    def __init__(self, *args, user=None, **kwargs):
        super(APIKeyForm, self).__init__(*args, **kwargs)
        if user is not None:
            self.fields["shop"].queryset = Shop.objects.filter(user=user).all()

    class Meta:
        model = APIKey
        fields = ["shop", "type", "name", "client_id", "client_secret", "is_active"]
        labels = {
            "shop": "Магазин",
            "type": "Тип ключа",
            "is_active": "Включен?",
        }

    def clean(self):
        """Проверяем дубликат ключа в системе и запрещаем добавление

        :return:
        """
        cleaned_data = super().clean()
        client_id = cleaned_data.get("client_id", "")
        client_secret = cleaned_data.get("client_secret", "")
        is_active = cleaned_data.get("is_active", False)

        api_keys = APIKey.objects.filter(
            ~Q(pk=self.instance.pk),
            client_id=client_id,
            client_secret=client_secret,
            is_active=True,
        )
        if len(api_keys) and is_active:
            raise ValidationError(
                f"Client_Id {client_id} с указанным Client_Secret уже используется в системе"
            )
        return cleaned_data


class SelfbuyForm(forms.ModelForm):
    class Meta:
        model = Selfbuy
        fields = ["shop", "order"]
        labels = {
            "shop": "Магазин",
            "order": "Номер заказа",
            "dt_buy": "Дата выкупа",
            "dt_take": "Дата начисления",
        }

    def __init__(self, *args, user=None, **kwargs):
        super(SelfbuyForm, self).__init__(*args, **kwargs)
        if user is not None:
            self.fields["shop"].queryset = Shop.objects.filter(user=user).all()
