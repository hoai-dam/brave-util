package waterplan;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class NoDirtWater implements WaterCubic {

    Boolean salty;
    final Boolean dirt = false;
    Boolean contaminated;
    Boolean heavyMetal;
    String characteristic;


    public NoDirtWater(WaterCubic waterCubic) {
        this.salty = waterCubic.getSalty();
        this.contaminated = waterCubic.getContaminated();
        this.heavyMetal = waterCubic.getHeavyMetal();
        this.characteristic = waterCubic.getCharacteristic();
    }

}
