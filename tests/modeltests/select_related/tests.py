from django.test import TestCase
from django.conf import settings
from django import db

from models import Domain, Kingdom, Phylum, Klass, Order, Family, Genus, Species

class SelectRelatedTests(TestCase):

    def create_tree(self, stringtree):
        """
        Helper to create a complete tree.
        """
        names = stringtree.split()
        models = [Domain, Kingdom, Phylum, Klass, Order, Family, Genus, Species]
        assert len(names) == len(models), (names, models)

        parent = None
        for name, model in zip(names, models):
            try:
                obj = model.objects.get(name=name)
            except model.DoesNotExist:
                obj = model(name=name)
            if parent:
                setattr(obj, parent.__class__.__name__.lower(), parent)
            obj.save()
            parent = obj

    def create_base_data(self):
        self.create_tree("Eukaryota Animalia Anthropoda Insecta Diptera Drosophilidae Drosophila melanogaster")
        self.create_tree("Eukaryota Animalia Chordata Mammalia Primates Hominidae Homo sapiens")
        self.create_tree("Eukaryota Plantae Magnoliophyta Magnoliopsida Fabales Fabaceae Pisum sativum")
        self.create_tree("Eukaryota Fungi Basidiomycota Homobasidiomycatae Agaricales Amanitacae Amanita muscaria")

    def setUp(self):
        # The test runner sets settings.DEBUG to False, but we want to gather
        # queries so we'll set it to True here and reset it at the end of the
        # test case.
        self.create_base_data()
        settings.DEBUG = True
        db.reset_queries()

    def tearDown(self):
        settings.DEBUG = False

    def test_access_fks_without_select_related(self):
        """
        Normally, accessing FKs doesn't fill in related objects
        """
        fly = Species.objects.get(name="melanogaster")
        domain = fly.genus.family.order.klass.phylum.kingdom.domain
        self.assertEqual(domain.name, 'Eukaryota')
        self.assertEqual(len(db.connection.queries), 8)

    def test_access_fks_with_select_related(self):
        """
        A select_related() call will fill in those related objects without any
        extra queries
        """
        person = Species.objects.select_related(depth=10).get(name="sapiens")
        domain = person.genus.family.order.klass.phylum.kingdom.domain
        self.assertEqual(domain.name, 'Eukaryota')
        self.assertEqual(len(db.connection.queries), 1)

    def test_list_without_select_related(self):
        """
        select_related() also of course applies to entire lists, not just
        items. This test verifies the expected behavior without select_related.
        """
        world = Species.objects.all()
        families = [o.genus.family.name for o in world]
        self.assertEqual(families, [
            'Drosophilidae',
            'Hominidae',
            'Fabaceae',
            'Amanitacae',
        ])
        self.assertEqual(len(db.connection.queries), 9)

    def test_list_with_select_related(self):
        """
        select_related() also of course applies to entire lists, not just
        items. This test verifies the expected behavior with select_related.
        """
        world = Species.objects.all().select_related()
        families = [o.genus.family.name for o in world]
        self.assertEqual(families, [
            'Drosophilidae',
            'Hominidae',
            'Fabaceae',
            'Amanitacae',
        ])
        self.assertEqual(len(db.connection.queries), 1)

    def test_depth(self, depth=1, expected=7):
        """
        The "depth" argument to select_related() will stop the descent at a
        particular level.
        """
        pea = Species.objects.select_related(depth=depth).get(name="sativum")
        self.assertEqual(
            pea.genus.family.order.klass.phylum.kingdom.domain.name,
            'Eukaryota'
        )
        # Notice: one fewer queries than above because of depth=1
        self.assertEqual(len(db.connection.queries), expected)

    def test_larger_depth(self):
        """
        The "depth" argument to select_related() will stop the descent at a
        particular level.  This tests a larger depth value.
        """
        self.test_depth(depth=5, expected=3)

    def test_list_with_depth(self):
        """
        The "depth" argument to select_related() will stop the descent at a
        particular level. This can be used on lists as well.
        """
        world = Species.objects.all().select_related(depth=2)
        orders = [o.genus.family.order.name for o in world]
        self.assertEqual(orders,
            ['Diptera', 'Primates', 'Fabales', 'Agaricales'])
        self.assertEqual(len(db.connection.queries), 5)

    def test_select_related_with_extra(self):
        s = Species.objects.all().select_related(depth=1)\
            .extra(select={'a': 'select_related_species.id + 10'})[0]
        self.assertEqual(s.id + 10, s.a)

    def test_certain_fields(self):
        """
        The optional fields passed to select_related() control which related
        models we pull in. This allows for smaller queries and can act as an
        alternative (or, in addition to) the depth parameter.

        In this case, we explicitly say to select the 'genus' and
        'genus.family' models, leading to the same number of queries as before.
        """
        world = Species.objects.select_related('genus__family')
        families = [o.genus.family.name for o in world]
        self.assertEqual(families,
            ['Drosophilidae', 'Hominidae', 'Fabaceae', 'Amanitacae'])
        self.assertEqual(len(db.connection.queries), 1)

    def test_more_certain_fields(self):
        """
        In this case, we explicitly say to select the 'genus' and
        'genus.family' models, leading to the same number of queries as before.
        """
        world = Species.objects.filter(genus__name='Amanita')\
            .select_related('genus__family')
        orders = [o.genus.family.order.name for o in world]
        self.assertEqual(orders, [u'Agaricales'])
        self.assertEqual(len(db.connection.queries), 2)

    def test_field_traversal(self):
        s = Species.objects.all().select_related('genus__family__order'
            ).order_by('id')[0:1].get().genus.family.order.name
        self.assertEqual(s, u'Diptera')
        self.assertEqual(len(db.connection.queries), 1)

    def test_depth_fields_fails(self):
        self.assertRaises(TypeError,
            Species.objects.select_related,
            'genus__family__order', depth=4
        )