"""ka_space URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.conf import settings
from django.contrib import admin
from django.contrib.auth.decorators import login_required
from django.views.decorators.cache import cache_page
from django.urls import path, re_path, include

from .views import register, ProfileView, DashboardView
from pages import views as pviews
from api import views as api_views
from ka_space.viewsets import MainView


urlpatterns = [
    path("", MainView.as_view()),
    path("api/", include(("ka_space.routers", "core"), namespace="core-api")),
    path("admin/", admin.site.urls),
]
