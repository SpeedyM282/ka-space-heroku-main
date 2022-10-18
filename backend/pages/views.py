from django.views.generic import TemplateView
from django.shortcuts import render
from django.http import HttpResponse


# class AboutView(TemplateView):
#    template_name = "about.html"


def index(request):
    data = {"title": "MP.Space by RAA", "content": ""}
    return render(request, "index.html", context=data)


def about(request):
    data = {"title": "О сайте", "content": ""}
    return render(request, "page.html", context=data)


def contact(request):
    data = {"title": "Контакты", "content": ""}
    return render(request, "page.html", context=data)


def help_connect(request):
    data = {"title": "Подключение", "content": ""}
    return render(request, "help_connect.html", context=data)
