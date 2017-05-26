<?php

namespace Woeplanet\Tests;

class PlacesTest extends \Woeplanet\Tests\UnitTest {
    private $places = null;

    protected function  setUp() {
        $this->places = new \Woeplanet\Types\Places();
    }

    public function testInstantiation() {
        $this->assertNotEquals(null, $this->places);
    }

    public function testGet() {
        $places = $this->places->get();
        $this->assertInternalType('array', $places);
        $this->assertEquals(31, count($places));
    }

    public function testByShortName() {
        $place = $this->places->get_by_shortname('Country');
        $this->validatePlace($place);
    }

    public function testById() {
        $place = $this->places->get_by_id(12);
        $this->validatePlace($place);
    }

    public function testByTag() {
        $place = $this->places->get_by_tag('country');
        $this->validatePlace($place);
    }

    protected function validatePlace($place) {
        error_log(var_export($place, true));
        $this->assertInternalType('array', $place);
        $this->assertEquals(2, count($place));
        $this->assertArrayHasKey('found', $place);
        $this->assertEquals(true, $place['found']);
        $this->assertArrayHasKey('placetype', $place);

        $this->assertArrayHasKey('id', $place['placetype']);
        $this->assertEquals(12, $place['placetype']['id']);

        $this->assertArrayHasKey('name', $place['placetype']);
        $this->assertEquals('Country', $place['placetype']['name']);

        $this->assertArrayHasKey('description', $place['placetype']);
        $this->assertEquals('One of the countries or dependent territories defined by the ISO 3166-1 standard', $place['placetype']['description']);

        $this->assertArrayHasKey('shortname', $place['placetype']);
        $this->assertEquals('Country', $place['placetype']['shortname']);

        $this->assertArrayHasKey('tag', $place['placetype']);
        $this->assertEquals('country', $place['placetype']['tag']);

    }
}
?>
