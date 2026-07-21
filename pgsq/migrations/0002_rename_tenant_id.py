from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("pgsq", "0001_initial"),
    ]

    operations = [
        migrations.RenameField(
            model_name="pgsqtask",
            old_name="username",
            new_name="tenant_id",
        ),
        migrations.RenameField(
            model_name="pgsqtaskslot",
            old_name="username",
            new_name="tenant_id",
        ),
    ]